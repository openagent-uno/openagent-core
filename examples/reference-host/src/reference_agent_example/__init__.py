"""An external identity domain and fixed catalog assembled through public APIs."""
from .host import Document, IdentityAdapter, ReferenceHost, create_app

__all__ = ["Document", "IdentityAdapter", "ReferenceHost", "create_app"]
