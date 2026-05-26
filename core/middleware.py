class RefreshDynamicAdminsMiddleware:
    """Re-register dynamic model admins on each admin request.

    Dynamic models are created by pipeline.py in a separate process,
    so the server must refresh its admin registry to pick up new ones.
    """

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        if request.path.startswith('/admin/'):
            from core.admin import register_dynamic_admins
            register_dynamic_admins()
        return self.get_response(request)
