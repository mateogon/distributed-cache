# urls.py
from django.urls import path
from . import views


urlpatterns = [
    path('<str:olid>/', views.book_details_view, name='book_details_view'),
]