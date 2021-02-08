from django.shortcuts import render
from psychotherapists.models import Psychotherapist,Photo,Thumbnail

# Create your views here.
def psychotherapists(request):
    therapists = Psychotherapist.objects.all() #query the table and get a queryset
    photos = Photo.objects.all()
    
    data = zip(therapists,photos)
    #The context dictionary is used to send information to our template. Every view function you create needs to have a context dictionary.
    context = {
        'data': data,
    }
    return render(request, 'psychotherapists.html', context)

#pk=id, to get the object with that id
def psychotherapist_detail(request, pk):
    therapist = Psychotherapist.objects.get(pk=pk)
    photo = Photo.objects.get(pk=therapist.photo_id)
    methods = therapist.methods.split(',')
    context = {
        'therapist': therapist,
        'photo': photo,
        'methods': methods,
    }
    return render(request, 'psychotherapist_detail.html', context)