from django.contrib.auth import authenticate, login, logout
from django.shortcuts import render
from django.views import View
from django.shortcuts import redirect
from django.urls import reverse


class LoginView(View):
    def get(self, request):
        if request.user.is_authenticated:
            print("Redirect user to dashboard")
        return render(request, 'login/index.html')

    def post(self, request):
        username = request.POST.get("username")
        password = request.POST.get("password")

        user = authenticate(username=username, password=password)
        if user and user.is_active:
            login(request, user)
            print("Redirect user to dashboard")

        return render(request, 'login/index.html')


def logout_view(request):
    logout(request)
    return redirect(reverse("login-view"))
