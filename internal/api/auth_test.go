package api

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func authStatus(h http.Handler, path, remote string, basic bool) int {
	r := httptest.NewRequest("GET", path, nil)
	r.RemoteAddr = remote
	if basic {
		r.SetBasicAuth("user", "pass")
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w.Code
}

var registerOnce sync.Once

// registerRoutes puts what Init registers, with an ONVIF base path below /onvif/, on the default mux.
func registerRoutes() {
	registerOnce.Do(func() {
		h := func(w http.ResponseWriter, r *http.Request) {}
		for _, pattern := range []string{"/", "/onvif/", "/api/streams", "/onvif/ui/api/config", "/onvif/ui/api/exit", "/onvif/ui/api/ws"} {
			http.HandleFunc(pattern, h)
		}
	})
}

func withOwnAuth(t *testing.T, patterns ...string) {
	t.Helper()
	registerRoutes()
	ownAuthMu.Lock()
	old := ownAuth
	ownAuth = nil
	ownAuthMu.Unlock()
	t.Cleanup(func() {
		ownAuthMu.Lock()
		ownAuth = old
		ownAuthMu.Unlock()
	})
	for _, p := range patterns {
		OwnAuth(p)
	}
}

func TestMiddlewareAuthOwnAuth(t *testing.T) {
	withOwnAuth(t, "/onvif/")

	ok := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	h := middlewareAuth("user", "pass", false, ok)
	far := "192.168.1.2:5000"

	require.Equal(t, 200, authStatus(h, "/onvif/device_service", far, false))
	require.Equal(t, 200, authStatus(h, "/onvif/ptz_service", far, false))
	require.Equal(t, 401, authStatus(h, "/api/streams", far, false))
	require.Equal(t, 401, authStatus(h, "/onvif", far, false), "not the pattern")
	require.Equal(t, 401, authStatus(h, "/other/onvif/x", far, false))
	require.Equal(t, 200, authStatus(h, "/api/streams", far, true))
	require.Equal(t, 200, authStatus(h, "/api/streams", "127.0.0.1:5000", false))

	// the static handler and the routes below /onvif/ (a base_path) keep Basic
	for _, target := range []string{
		"/video-rtc.js",
		"/onvif%2f..%2fvideo-rtc.js",
		"/onvif%2f../video-rtc.js",
		"/onvif/../video-rtc.js",
		"/onvif//device_service",
		"/onvif/ui/api/config",
		"/onvif/ui/api/exit",
		"/onvif/ui/api/ws",
	} {
		require.Equal(t, 401, authStatus(h, target, far, false), target)
		require.Equal(t, 200, authStatus(h, target, far, true), target)
	}

	h = middlewareAuth("user", "pass", true, ok)
	require.Equal(t, 401, authStatus(h, "/api/streams", "127.0.0.1:5000", false))
	require.Equal(t, 200, authStatus(h, "/onvif/device_service", "127.0.0.1:5000", false))
}

func TestMiddlewareAuthWithoutOwnAuth(t *testing.T) {
	withOwnAuth(t)

	h := middlewareAuth("user", "pass", false, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	require.Equal(t, 401, authStatus(h, "/onvif/device_service", "192.168.1.2:5000", false))
}

func TestAuth(t *testing.T) {
	oldU, oldP, oldL := authUser, authPass, authLocal
	t.Cleanup(func() { authUser, authPass, authLocal = oldU, oldP, oldL })
	authUser, authPass, authLocal = "u", "p", true

	u, p, l := Auth()
	require.Equal(t, "u", u)
	require.Equal(t, "p", p)
	require.True(t, l)
}

func TestIsLoopback(t *testing.T) {
	require.True(t, IsLoopback("127.0.0.1:1"))
	require.True(t, IsLoopback("[::1]:1"))
	require.True(t, IsLoopback("@"))
	require.False(t, IsLoopback("192.168.1.2:1"))
}

func TestOwnAuthConcurrent(t *testing.T) {
	withOwnAuth(t)

	ok := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
	h := middlewareAuth("user", "pass", false, ok)

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				authStatus(h, "/onvif/device_service", "192.168.1.2:5000", false)
				authStatus(h, "/api/streams", "192.168.1.2:5000", false)
			}
		}()
	}
	for i := 0; i < 50; i++ {
		OwnAuth("/onvif/")
	}
	wg.Wait()

	require.Equal(t, 200, authStatus(h, "/onvif/device_service", "192.168.1.2:5000", false))
	require.Equal(t, 401, authStatus(h, "/api/streams", "192.168.1.2:5000", false))
}
