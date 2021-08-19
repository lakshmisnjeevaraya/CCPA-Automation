import logtobq
#from ccpa_slingbox_delete import ccpa_slingbox_delete
# from ccpa_conviva_report import ccpa_conviva_request
from ccpa_conviva_report_old import ccpa_conviva_request

from ccpa_slingbox_report import ccpa_slingbox_request
from ccpa_dany_report import ccpa_dany_request
from ccpa_dany_delete import ccpa_dany_delete_request
from ccpa_conviva_report_new import  ccpa_conviva_request

if __name__ == '__main__':
    # ccpa_slingbox_delete()
    #  ccpa_conviva_report.ccpa_conviva_request()
    ccpa_conviva_request()

    # # ccpa_conviva_request()
    # ccpa_dany_request()
    # # ccpa_slingbox_request()
    # ccpa_dany_delete_request()
    logtobq.loadlogfiletobigquery()