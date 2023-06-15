
from rest_framework import viewsets
from .models import Data
from .serializers import DataSerializer 
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework import status
from datetime import timedelta ,datetime
from django.db.models import Avg
from django.utils import timezone
class DataViewSet(viewsets.ModelViewSet):
    serializer_class =DataSerializer
    queryset = Data.objects.all()


    @action(detail=False)
    def get_last(self, request):
        


        # Extraction des données toutes les 15 minutes
        data = Data.objects.last() 

        serializer = self.get_serializer(data)
        return Response(serializer.data, status=status.HTTP_200_OK)
    
    @action(detail=False)
    def get_last_15(self, request):


        # Extraction des données toutes les 15 minutes
        time = datetime.now() - timedelta(minutes=15)
        data = Data.objects.filter(timestamp__gte=time)
        print(data)
        serializer = self.get_serializer(data ,many=True)


        return Response(serializer.data, status=status.HTTP_200_OK)
    
    @action(detail=False)
    def get_moy_hour(self, request):

        # Moyennes par heure

        	
        time = datetime.now() - timedelta(hours=1)
        data_by_hour = Data.objects.filter(timestamp__gte=time).aggregate(Avg('temperature'))
        print(data_by_hour)
        return Response({"temperature":data_by_hour['temperature__avg']}, status=status.HTTP_200_OK)
    
    @action(detail=False)
    def get_moy_day(self, request):
        time = datetime.now() - timedelta(days=1)
        data_by_day = Data.objects.filter(timestamp__gte=time).aggregate(Avg('temperature'))

        return Response({"temperature":data_by_day['temperature__avg']}, status=status.HTTP_200_OK)