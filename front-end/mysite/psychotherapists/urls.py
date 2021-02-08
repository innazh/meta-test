from django.urls import path
from . import views

urlpatterns = [
    path("", views.psychotherapists, name="psychotherapists.html"),
    #the val assigned to url is of type integer and we pass it down to our handler as pk
    path("<str:pk>/", views.psychotherapist_detail, name="psychotherapist_detail"),
]