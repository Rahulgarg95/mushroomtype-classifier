import pandas
from application_logging.logger import App_Logger
from AwsS3Storage.awsStorageManagement import AwsStorageManagement

class dataTransform:

     """
          This class shall be used for transforming the Good Raw Training Data before loading it in Database!!.
     """

     def __init__(self):
          self.goodDataPath = "Training_Good_Raw_Files_Validated"
          self.logger = App_Logger()
          self.awsObj = AwsStorageManagement()


     def addQuotesToStringValuesInColumn(self):
          """
             Method Name: addQuotesToStringValuesInColumn
             Description: This method converts all the columns with string datatype such that
                         each value for that column is enclosed in quotes. This is done
                         to avoid the error while inserting string values in table as varchar.
          """

          log_file = 'addQuotesToStringValuesInColumn'
          try:
               onlyfiles = self.awsObj.listDirFiles(self.goodDataPath)
               for file in onlyfiles:
                    data = self.awsObj.csvToDataframe(self.goodDataPath, file)
                    for column in data.columns:
                         count = data[column][data[column] == '?'].count()
                         if count != 0:
                              data[column] = data[column].replace('?', "'?'")
                    self.awsObj.saveDataframeToCsv(self.goodDataPath, file, data)
                    self.logger.log(log_file," %s: Quotes added successfully!!" % file)
          except Exception as e:
               self.logger.log(log_file, "Data Transformation failed because:: %s" % e)