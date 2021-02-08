from django.db import models

class Psychotherapist(models.Model):
    ph_id = models.CharField(max_length=100, primary_key=True,db_column='id')#might be danger
    createdTime = models.CharField(max_length=100)
    methods = models.CharField(max_length=200,db_column='fields.Методы')
    name = models.CharField(max_length=100,db_column='fields.Имя')
    photo_id = models.CharField(max_length=100,db_column='fields.PhotoId')

    class Meta:
        managed = False
        db_table = 'psychotherapists'

class Photo(models.Model):
    p_id = models.CharField(max_length=100, primary_key=True,db_column='id')
    p_url = models.CharField(max_length=200,db_column='url')
    p_filename = models.CharField(max_length=100,db_column='filename')
    p_size = models.BigIntegerField(db_column='size')#as its bigint in db
    p_type = models.CharField(max_length=20,db_column='type')

    class Meta:
        managed = False
        db_table = 'photos'

class Thumbnail(models.Model):
    photo_id = models.CharField(max_length=100, primary_key=True,db_column='photo_id')
    t_type = models.CharField(max_length=10,db_column='type')
    t_url = models.CharField(max_length=200,db_column='url')
    t_width = models.BigIntegerField(db_column='width')
    t_height = models.BigIntegerField(db_column='height')

    class Meta:
        managed = False
        db_table = 'thumbnails'

    def __repr__(self):
        return f'<Thumbnail: Thumbnail object ({self.p_id}, {self.t_type}>'