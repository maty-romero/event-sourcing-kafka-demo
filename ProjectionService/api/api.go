package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"

	"event_projection/store"
)

// Run levanta el servidor HTTP de lecturas del read model. Cada proyeccion es
// la unica duena de su base: los clientes consultan aca, no a la API de comandos.
func Run(ctx context.Context, addr string, db *sql.DB, kind string) error {
	mux := http.NewServeMux()

	if kind == store.KindBalances {
		mux.HandleFunc("GET /accounts/{id}/balance", func(w http.ResponseWriter, r *http.Request) {
			id := r.PathValue("id")
			balance, found, err := store.LookupBalance(r.Context(), db, id)
			if err != nil {
				http.Error(w, `{"error":"lectura de balance fallo"}`, http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			if !found {
				w.WriteHeader(http.StatusNotFound)
				_ = json.NewEncoder(w).Encode(map[string]any{"accountId": id, "error": "account not found"})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"accountId": id, "balance": balance})
		})
	}

	if kind == store.KindStatement {
		mux.HandleFunc("GET /accounts/{id}/movements", func(w http.ResponseWriter, r *http.Request) {
			id := r.PathValue("id")
			movements, err := store.LookupMovements(r.Context(), db, id)
			if err != nil {
				http.Error(w, `{"error":"lectura de movimientos fallo"}`, http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"accountId": id, "movements": movements})
		})
	}

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown HTTP: %v", err)
		}
	}()

	log.Printf("HTTP de lecturas (%s) escuchando en %s", kind, addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
