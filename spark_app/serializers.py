from rest_framework import serializers
from .models import Data 




class DataSerializer(serializers.ModelSerializer):

    class Meta:
        model = Data 
        fields = ('id','temperature','humidity','wind_speed','timestamp')


