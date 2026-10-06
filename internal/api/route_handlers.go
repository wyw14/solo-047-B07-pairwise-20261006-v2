package api

import (
	"net/http"

	"github.com/go-chi/chi/v5"
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
		// Persist the terminal snapshot even on failure: the operation's
		// event trail is the audit record of why topology did not change.
		_ = s.services.Persist("switchover", result.ID, result)
		writeError(writer, http.StatusConflict, err)
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
