from django.db import models

class BookingData(models.Model):
    bank_code = models.IntegerField()  # Bank numeric code
    txn_date = models.DateField(blank=True, null=True)  # Change to DateField for proper date handling
    irctc_order_no = models.BigIntegerField(blank=True, null=True)  # Use BigIntegerField if expecting large order numbers
    bank_booking_ref_no = models.BigIntegerField(blank=True, null=True)  # Change to BigIntegerField
    booking_amount = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)  # Change to DecimalField
    credited_date = models.DateField(blank=True, null=True)  # Change to DateField for consistency
    cus_account_no = models.CharField(max_length=25, blank=True, null=True)
    remarks = models.CharField(max_length=25, blank=True, null=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=['txn_date', 'bank_code', 'credited_date', 'irctc_order_no', 'bank_booking_ref_no'], name='unique_bookingdata_constraint')
        ]

class RefundData(models.Model):
    bank_code = models.IntegerField()  # Bank numeric code
    refund_date = models.DateField(blank=True, null=True)  # Change to DateField for proper date handling
    irctc_order_no = models.BigIntegerField(blank=True, null=True)  # Use BigIntegerField if expecting large order numbers
    bank_booking_ref_no = models.BigIntegerField(blank=True, null=True)  # Change to BigIntegerField
    bank_refund_ref_no = models.BigIntegerField(blank=True, null=True)  # Change to BigIntegerField
    refund_amount = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)  # Change to DecimalField
    debited_date = models.DateField(blank=True, null=True)  # Change to DateField for consistency
    cus_account_no = models.CharField(max_length=25, blank=True, null=True)
    remarks = models.CharField(max_length=25, blank=True, null=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=['refund_date', 'bank_code', 'debited_date', 'irctc_order_no', 'bank_booking_ref_no', 'bank_refund_ref_no'], name='unique_refunddata_constraint')
        ]
