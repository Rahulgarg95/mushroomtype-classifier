import pandas as pd
from application_logging.logger import App_Logger
from AwsS3Storage.awsStorageManagement import AwsStorageManagement

class dataTransformPredict:

     """
          This class shall be used for transforming the Good Raw Training Data before loading it in Database!!.
     """

     def __init__(self):
          self.goodDataPath = "Prediction_Good_Raw_Files_Validated"
          self.logger = App_Logger()
          self.awsObj = AwsStorageManagement()


     def addQuotesToStringValuesInColumn(self):

          """
              Method Name: addQuotesToStringValuesInColumn
              Description: This method replaces the missing values in columns with "NULL" to
                           store in the table. We are using substring in the first column to
                           keep only "Integer" data for ease up the loading.
                           This column is anyways going to be removed during prediction.
          """

          try:
               log_file = 'dataTransformLog'
               onlyfiles = self.awsObj.listDirFiles(self.goodDataPath)
               for file in onlyfiles:
                    data = self.awsObj.csvToDataframe(self.goodDataPath, file)
                    data['stalk-root'] = data['stalk-root'].replace('?', "'?'")
                    self.awsObj.saveDataframeToCsv(self.goodDataPath, file, data)
                    self.logger.log(log_file, " %s: Quotes added successfully!!" % file)
          except Exception as e:
               log_file = 'dataTransformLog'
               self.logger.log(log_file, "Data Transformation failed because:: %s" % e)
               raise e