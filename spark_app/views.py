
from rest_framework import viewsets
from .models import Data
from .serializers import DataSerializer 

class DataViewSet(viewsets.ModelViewSet):
    serializer_class =DataSerializer
    queryset = Data.objects.all()


