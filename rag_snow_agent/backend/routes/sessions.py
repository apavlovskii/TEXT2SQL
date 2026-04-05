"""Session management routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from ..dependencies import get_session_store
from ..models.requests import SessionCreate, SessionRename
from ..models.responses import SessionDetailResponse, SessionResponse
from ..services.session_store import SessionStore

router = APIRouter(prefix="/api/sessions", tags=["sessions"])


@router.get("", response_model=list[SessionResponse])
def list_sessions(store: SessionStore = Depends(get_session_store)):
    return store.list_sessions()


@router.post("", response_model=SessionResponse, status_code=201)
def create_session(
    body: SessionCreate,
    store: SessionStore = Depends(get_session_store),
):
    return store.create_session(name=body.name, db_id=body.db_id)


@router.get("/{session_id}", response_model=SessionDetailResponse)
def get_session(
    session_id: str,
    store: SessionStore = Depends(get_session_store),
):
    session = store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return session


@router.delete("/{session_id}", status_code=204)
def delete_session(
    session_id: str,
    store: SessionStore = Depends(get_session_store),
):
    if not store.delete_session(session_id):
        raise HTTPException(status_code=404, detail="Session not found")


@router.patch("/{session_id}", response_model=SessionResponse)
def rename_session(
    session_id: str,
    body: SessionRename,
    store: SessionStore = Depends(get_session_store),
):
    if not store.rename_session(session_id, body.name):
        raise HTTPException(status_code=404, detail="Session not found")
    session = store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return session
