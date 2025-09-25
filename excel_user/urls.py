from django.urls import path
from . import views

urlpatterns = [
    path('', views.index, name='index'),  # example view
    path('view_excel_data',views.view_excel_data,name='view_excel_data')
]