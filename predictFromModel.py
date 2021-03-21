import pandas
from file_operations import file_methods
from data_preprocessing import preprocessing
from data_ingestion import data_loader_prediction
from application_logging import logger
from Prediction_Raw_Data_Validation.predictionDataValidation import Prediction_Data_validation
from AwsS3Storage.awsStorageManagement import AwsStorageManagement
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from Email_Trigger.send_email import email
from datetime import datetime


class prediction:

    def __init__(self,path):
        self.file_object = 'Prediction_Log'
        self.log_writer = logger.App_Logger()
        self.awsObj = AwsStorageManagement()
        self.emailObj = email()
        if path is not None:
            self.pred_data_val = Prediction_Data_validation(path)

    def predictionFromModel(self):

        try:
            self.pred_data_val.deletePredictionFile() #deletes the existing prediction file from last run!
            self.log_writer.log(self.file_object,'Start of Prediction')
            data_getter=data_loader_prediction.Data_Getter_Pred(self.file_object,self.log_writer)
            data=data_getter.get_data()

            preprocessor=preprocessing.Preprocessor(self.file_object,self.log_writer)
            data = preprocessor.dropUnnecessaryColumns(data,['veil-type'])

            # replacing '?' values with np.nan as discussed in the EDA part

            data = preprocessor.replaceInvalidValuesWithNull(data)

            is_null_present,cols_with_missing_values=preprocessor.is_null_present(data)
            if(is_null_present):
                data=preprocessor.impute_missing_values(data,cols_with_missing_values)

            # get encoded values for categorical data
            data = preprocessor.encodeCategoricalValuesPrediction(data)

            #data=data.to_numpy()
            file_loader=file_methods.File_Operation(self.file_object,self.log_writer)
            kmeans=file_loader.load_model('KMeans')
            print(kmeans.labels_)
            ##Code changed
            #pred_data = data.drop(['Wafer'],axis=1)
            clusters=kmeans.predict(data)
            data['clusters']=clusters
            clusters=data['clusters'].unique()
            result=[] # initialize blank list for storing predicitons
            # with open('EncoderPickle/enc.pickle', 'rb') as file: #let's load the encoder pickle file to decode the values
            #     encoder = pickle.load(file)

            for i in clusters:
                cluster_data= data[data['clusters']==i]
                cluster_data = cluster_data.drop(['clusters'],axis=1)
                model_name = file_loader.find_correct_model_file(i)
                model = file_loader.load_model(model_name)
                for val in (model.predict(cluster_data)):
                    result.append(val)
            result = pandas.DataFrame(result,columns=['Predictions'])
            path="Prediction_Output_File/Predictions.csv"
            self.awsObj.saveDataframeToCsv('Prediction_Output_File', 'Predictions.csv', result)
            self.log_writer.log(self.file_object,'End of Prediction')

            msg = MIMEMultipart()
            msg['Subject'] = 'MushroomTypeClassifier - Prediction Done | ' + str(datetime.now())
            body = 'Model Prediction Done Successfully... <br><br> Thanks and Regards, <br> Rahul Garg'
            msg.attach(MIMEText(body, 'html'))
            to_addr = ['rahulgarg366@gmail.com']
            self.emailObj.trigger_mail(to_addr, [], msg)
        except Exception as ex:
            self.log_writer.log(self.file_object, 'Error occured while running the prediction!! Error:: %s' % ex)
            raise ex
        return path