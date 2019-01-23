
def monthly_reports(_content_owner,_table):
    print("----------------- Checking Monthly Report --------------------")
    newest_month_BG = get_lastest_month_func(content_owner=_content_owner, table=_table)
    print("--------------------------" + _table + "--------------------------")
    _retrieve_reports_monthly = retrieve_reports(youtube_reporting, lastest_date_BQ=newest_month_BG,
                                                 jobId=_content_owner[str(_table)],
                                                 onBehalfOfContentOwner=content_owner[str(_content_owner)])
    if bool(_retrieve_reports_monthly) == True:
        print("----- The newest_date_on_BQ: {0} but {1} on API ".format(newest_month_BG,
                                                                        max(_retrieve_reports_monthly.keys())))
        print("---------- ", len(_retrieve_reports_monthly), " reports need to retrieve ----------")
        for date_retreive, value_retrieve in _retrieve_reports_monthly.items():
            print("-------- " + _table + " Updating date: ", date_retreive, " -------------------")
            report_url = value_retrieve[1]
            download_report(youtube_reporting, report_url, content_owner+"_"+_table+"_"+str(date_retreive).split(" ")[0]+".txt")
            for _file in check_files(FOLDER_PATH):
                if "p_content_owner_ad_revenue_raw_a1" in _file and _file == _content_owner + "_" + _table + "_" + \
                        str(date_retreive).split(" ")[0] + ".txt":
                    try:
                        p_content_owner_ad_revenue_raw_a1(_file, _table,_content_owner)
                        print("{0} Inserted successfully".format(_table))
                        os.remove(_file)
                    except (TypeError) as e:
                        pass
                else:
                    pass
        print("---------------------" + _content_owner + " - " + _table + " DONE! ---------------------")
    else:
        print("-----" + _content_owner + " - " + _table + " is up to date!")