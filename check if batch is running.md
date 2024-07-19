
## Check if batch is running to terminate it.
### You can check the jobcount and jobname at the button "Job details" in t-code SM37. It shows ID and Job Name.

Syntax:
``` abap
IF sy-batch = abap_true.
  CALL FUNCTION 'BP_JOB_STATUS_GET'
    EXPORTING
      jobcount                  = jobcount
      jobname                   = jobname
    IMPORTING
      status                    = status
    EXCEPTIONS
      job_doesnt_existt         = 1
      unkonw_error              = 2
      parent_child_incoststency = 3
      OTHERS                    = 4.
  IF sy-subrc <> 0.
    MESSAGE ID sy-msgid TYPE sy-msgty NUMBER sy-msgno
      WITH sy-msgv1 sy-msgv2 sy-msgv3 sy-msgv4.
    RETURN.
  ELSEIF status = 'R'. " Acting(running)
    MESSAGE `Batch is already running`  TYPE 'E'.
  ENDIF.
```
