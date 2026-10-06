package api

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/wyw14/cry-103/internal/switchover"
)

func (s *Server) startSwitchover(writer http.ResponseWriter, request *http.Request) {
	routeID := chi.URLParam(request, "id")
	target, err := s.services.Route(routeID)
	if err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	var input SwitchoverRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	result, err := s.services.Switchovers.Failover(request.Context(), target)
	if err != nil {
		// Persist the failed operation as well: its stage events are the audit
		// trail of why the topology did (or did not) change.
		if result.ID != "" {
			if persistErr := s.services.Persist("switchover", result.ID, result); persistErr != nil {
				writeError(writer, http.StatusInternalServerError, persistErr)
				return
			}
		}
		switch {
		case errors.Is(err, switchover.ErrRetryable):
			writeError(writer, http.StatusServiceUnavailable, err)
		default:
			// Non-recoverable device/topology fault; no topology change.
			writeError(writer, http.StatusBadGateway, err)
		}
		return
	}
	if err := s.services.Persist("switchover", result.ID, result); err != nil {
		writeError(writer, http.StatusInternalServerError, err)
		return
	}
	writeJSON(writer, http.StatusAccepted, result)
}

func (s *Server) getRoute(writer http.ResponseWriter, request *http.Request) {
	target, err := s.services.Route(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	writeJSON(writer, http.StatusOK, target.Snapshot())
}
