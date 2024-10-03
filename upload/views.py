

from django.shortcuts import render, redirect
from .forms import UploadFileForm
from .tasks import process_uploaded_files
from django.http import HttpResponse
from .models import BookingData, RefundData
from celery.result import AsyncResult
from django.http import JsonResponse
from django.db import connection

def upload_files(request):
    if request.method == 'POST':
        # Assume you have a file input named 'file' in your HTML form
        uploaded_file = request.FILES['file']
        bank_name = request.POST.get('bank_name')  # Get bank name from the form
        transaction_type = request.POST.get('transaction_type')  # Get transaction type from the form

        # Read file content
        file_content = uploaded_file.read()
        file_name = uploaded_file.name
        
        task_ids = []

        # Trigger the Celery task to process the file
        result = process_uploaded_files.delay(file_content, file_name, bank_name, transaction_type)
        
        task_ids.append(result.id)  # Collect task IDs

        # Store task IDs in session
        request.session['task_ids'] = task_ids

        return HttpResponse("File has been uploaded and is being processed")

        # return redirect('display_data')  # Redirect to success page

    else:
        form = UploadFileForm()

    return render(request, 'upload.html', {'form': form})

def check_task_status(request):
    task_ids = request.session.get('task_ids', [])
    task_statuses = []

    for task_id in task_ids:
        result = AsyncResult(task_id)
        status = result.status
        result_value = result.result if result.ready() else 'Not ready yet'
        task_statuses.append({
            'task_id': task_id,
            'status': status,
            'result': result_value
        })

    return render(request, 'task_status.html', {'task_statuses': task_statuses})

# def display_data(request):
#     bank_name = request.GET.get('bank_name')
#     year = request.GET.get('year')
#     month = request.GET.get('month')
#     booking_or_refund = request.GET.get('booking_or_refund')
#     date = request.GET.get('date')

#     # Fetch and compare data from production DB
#     unmatched_records = compare_db_data(bank_name, year, month)

#     # Filter the data based on user selection
#     data = BookingData.objects.filter(
#         bank_name=bank_name, 
#         year=year, 
#         month=month,
#         date=date,
#     )

#     return render(request, 'display_data.html', {'data': data, 'unmatched_records': unmatched_records})
