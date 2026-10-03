package prerender

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// 发布、失效与读取并发进行：读取结果必须自洽（Body 与 Version 始终匹配，
// 即只能看到某个完整版本），失效不得丢失刚提交的新版本。
func TestConcurrentPublishInvalidateRead(t *testing.T) {
	origin := &fakeOrigin{body: func(route string, v uint64) []byte {
		return []byte(fmt.Sprintf("%s-v%d", route, v))
	}}
	retry := RetryConfig{MaxAttempts: 2}
	store := NewContentStore()
	renderer := NewRenderer(origin, retry)
	cache := NewCache(NewMemoryBackend(), store, renderer, retry)
	purgeLog := NewPurgeLog()

	cfg := RouteConfig{
		Path:        "hot",
		Mode:        ModeISR,
		FreshTTL:    time.Millisecond, // 极短新鲜期，迫使频繁再生
		StaleTTL:    time.Minute,
		StalePolicy: StalePolicyServeStale,
		Tags:        []string{"t"},
	}

	const publishers = 3
	const readers = 8
	const rounds = 50

	var wg sync.WaitGroup
	errs := make(chan error, publishers*rounds+readers*rounds)

	// 发布者：渲染 -> 提交 -> 回填，版本号各自递增。
	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func(base uint64) {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				v := store.Version("hot") + 1
				page, err := renderer.Render(context.Background(), cfg, v)
				if err != nil {
					errs <- err
					return
				}
				store.Commit(page)
				_ = cache.Fill(page, cfg) // 与失效竞争时被拒绝是合法的
				_ = base
			}
		}(uint64(p))
	}

	// 失效者：按递增水位发事件（含重复与乱序）。
	wg.Add(1)
	go func() {
		defer wg.Done()
		var seq uint64
		for i := 0; i < rounds; i++ {
			v := store.Version("hot")
			seq++
			purgeLog.Apply(cache, PurgeEvent{Seq: seq, Tag: "t", Version: v})
			if i%3 == 0 { // 重复事件
				purgeLog.Apply(cache, PurgeEvent{Seq: seq, Tag: "t", Version: v})
			}
			if i%5 == 0 && v > 1 { // 乱序旧事件
				purgeLog.Apply(cache, PurgeEvent{Seq: seq + 1000, Tag: "t", Version: v - 1})
			}
		}
	}()

	// 读取者：校验自洽性——返回内容的 Body 必须与其 Version 匹配。
	for r := 0; r < readers; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				page, st, err := cache.Get(context.Background(), cfg)
				if err != nil {
					if st == StatusError { // 再生竞争失败可接受，继续
						continue
					}
					errs <- fmt.Errorf("read: %w", err)
					return
				}
				want := fmt.Sprintf("hot-v%d", page.Version)
				if string(page.Body) != want {
					errs <- fmt.Errorf("inconsistent page: body=%q version=%d", page.Body, page.Version)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	// 收敛性：最终缓存（若存在）版本不得高于已提交版本，
	// 且已提交版本必须可读（失效不得丢失刚提交的新版本）。
	committed, ok := store.Committed("hot")
	if !ok {
		t.Fatal("expected committed version")
	}
	var page *Page
	var st CacheStatus
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		page, st, err = cache.Get(context.Background(), cfg)
		if err == nil {
			break
		}
	}
	if err != nil {
		t.Fatalf("final read: %v", err)
	}
	if page.Version < committed.Version && st != StatusStale {
		t.Fatalf("read version %d behind committed %d (st=%s)", page.Version, committed.Version, st)
	}
}

// 并发的 SSG 提交与读取：任何时刻读取方只能看到完整版本。
func TestConcurrentSSGCommitAndRead(t *testing.T) {
	store := NewContentStore()
	const commits = 200

	var committer sync.WaitGroup
	committer.Add(1)
	go func() {
		defer committer.Done()
		for v := uint64(1); v <= commits; v++ {
			store.Commit(&Page{
				Route:   "s",
				Mode:    ModeSSG,
				Version: v,
				Body:    []byte(fmt.Sprintf("s-v%d", v)),
			})
		}
	}()

	stop := make(chan struct{})
	var readers sync.WaitGroup
	for r := 0; r < 4; r++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				p, ok := store.Committed("s")
				if !ok {
					continue
				}
				if string(p.Body) != fmt.Sprintf("s-v%d", p.Version) {
					t.Errorf("torn read: body=%q version=%d", p.Body, p.Version)
					return
				}
			}
		}()
	}
	committer.Wait()
	close(stop)
	readers.Wait()
}
