"""
URL configuration for TaxComplianceProject project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/5.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path, include
from django.contrib.auth import views as auth_views

# Set the custom titles
admin.site.site_header = "Հայտարարագրերի ավտոմատացված հաշվարկի ադմին պանել"
admin.site.site_title = "Հայտարարագրերի ադմին պանել"
admin.site.index_title = "Բարի գալուստ հայտարարագրերի կառավարման պանել"

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', include('tax_processor.urls')),

    # --- AUTHENTICATION PATHS (NEW) ---
    path('login/', auth_views.LoginView.as_view(template_name='tax_processor/login.html'), name='login'),
    path('logout/', auth_views.LogoutView.as_view(), name='logout'),
]
