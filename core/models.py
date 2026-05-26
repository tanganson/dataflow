from django.db import models

# Create your models here.
class Dataset(models.Model):
    """ Dataset model """
    name = models.CharField(max_length=255)
    description = models.TextField()
    raw_data = models.JSONField(default=list, null=True, blank=True)
    updated_at = models.DateTimeField(auto_now_add=True)
    
    def __str__(self):
        return self.name
    
class DataRecord(models.Model):
    """ actual Dataset  """
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE)
    data = models.JSONField() # structure
    created_at = models.DateTimeField(auto_now_add=True)
    
    def __str__(self):
        return f'Record {self.id} '

class CleaningLog(models.Model):
    """Cleaning operation log for a dataset"""
    dataset = models.ForeignKey(Dataset, on_delete=models.CASCADE)
    status = models.CharField(max_length=50, default="pending")
    result = models.JSONField(default=dict)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"CleaningLog {self.id} - {self.status}"


class DatasetSchema(models.Model):
    """Typed schema defining the columns of a dataset's dynamic table."""
    dataset = models.OneToOneField(
        Dataset, on_delete=models.CASCADE, related_name='schema'
    )
    table_name = models.CharField(max_length=120, unique=True)
    fields_json = models.JSONField()
    version = models.PositiveIntegerField(default=1)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Schema for '{self.dataset.name}' -> {self.table_name}"