import pipeline.app.load.pre_load_formatter as plf

x = plf.PreLoadFormatter()

applicants['InvitorID'] = applicants['InvitorID'].map(invitors.set_index('InvitorName')['InvitorID'])