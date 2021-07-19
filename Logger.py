from datetime import datetime


class Logger:

    def __init__(self, file="logfile.txt"):
        self.f_name = file

    def log(self, log_type, log_msg):
        now = datetime.now()
        current_time = now.strftime("%d-%m-%Y %H:%M:%S")
        f = open(self.f_name, "a+")
        f.write(current_time+","+log_type+","+log_msg+"\n")
        f.close()
