from django.contrib.auth import authenticate, login, logout
from django.shortcuts import render
from django.views import View
from django.shortcuts import redirect
from django.urls import reverse


class LoginView(View):
    def get(self, request):
        if request.user.is_authenticated:
            return render(request, 'dashboard/index.html')
        return render(request, 'login/index.html')

    def post(self, request):
        username = request.POST.get("username")
        password = request.POST.get("password")

        user = authenticate(username=username, password=password)
        if user and user.is_active:
            login(request, user)
            return render(request, 'dashboard/index.html')

        context = {"message": "Your username or password was incorrect."}
        return render(request, 'login/index.html', context=context)


def logout_view(request):
    logout(request)
    return redirect(reverse("login-view"))
