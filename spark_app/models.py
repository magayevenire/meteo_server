from django.db import models
# from django.db.models import Avg, Count, Min, Sum


class Data(models.Model):
    city = models.CharField(max_length=255,null=True,blank=True)
    temperature = models.FloatField(null=True,blank=True)
    humidity = models.FloatField(null=True,blank=True)
    wind_speed = models.FloatField(null=True,blank=True)
    timestamp = models.TimeField(null=True,blank=True)


