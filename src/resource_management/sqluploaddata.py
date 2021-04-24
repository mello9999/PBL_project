from DBHelper import DBHelper
#Put this script to the same place where the data was placed.

#create class 
#for the example that we've made show that we've created 3 functions = behavior.csv ,sleep_div.csv , ML.csv 
class ScriptSQl:
    def __init__(self):
        self.db = DBHelper()

    def create_behavior_table(self, appetite, pref, sleep, anxiety, without_railing, walk15mins, actively_go_out, washing_yourself, shoping, no_wieght_loss, no_weight_gain, exercise_func, nutrition):
        
        self.db.execute( "CREATE TABLE behavior (SubjectID VARCHAR (50) PRIMARY KEY "
                            " , {} VARCHAR(50) "
                            " , {} VARCHAR(50) "
                            " , {} VARCHAR(50) "
                            " , {} VARCHAR(50) "
                            " , {} VARCHAR(50) " 
                            " , {} VARCHAR(50) " 
                            " , {} VARCHAR(50) "
                            " , {} VARCHAR(50) "
                            " , {} VARCHAR(50) "
                            " , {} VARCHAR(50) "
                            " , {} VARCHAR(50) "
                            " , {} VARCHAR(50) "
                            " , {} VARCHAR(50)); ".format(appetite, pref, sleep, anxiety, without_railing, walk15mins, actively_go_out, washing_yourself, shoping, no_wieght_loss, no_weight_gain, exercise_func, nutrition ))
        print("Finished creating behavior table") #don't forget to change arguments according to file csv that you want.

    def create_sleep_table(self, year, month, day, sleep_hour):
        
        self.db.execute( "CREATE TABLE sleep (SubjectID VARCHAR (50) "
                            " , {} VARCHAR(50) "
                            " , {} VARCHAR(50) "
                            " , {} VARCHAR(50) "
                            " , {} VARCHAR(50)); ".format(year, month, day, sleep_hour))
        print("Finished creating sleep table")
        
        
    def create_ML_table(self, ML):
    
        self.db.execute( "CREATE TABLE ML (SubjectID VARCHAR (50) "                            
                            " , {} VARCHAR(50)); ".format(ML))
        print("Finished creating ML table")     

    def upload(self, file, table):
        with open(file, 'r' , encoding="utf8") as f:
            next(f) # Skip the header row.
            self.db.copy(f, table, sep=',')

        print("Upload Success")

#run class ScriptSQL as the same as your table name.
#don't forget to insert column according to csv. 
ScriptSQl().create_behavior_table("Appetite_Questionnaire_results", "Preference_Questionnaire_results"
                    , "Sleep_Questionnaire_results", "Anxiety_about_health_Questionnaire_results"
                    , "I_can_go_up_and_down_stairs_without_being_transmitted_to_the_railing_or_wall"
                    , "I_can_walk_for_more_than_15_minutes"
                    , "I_am_actively_going_out" , "Do_the_cleaning_and_washing_yourself"
                    , "Shop_for_daily_necessities_yourself"
                    , "No_weight_loss_of_more_than_2_to_3_kg_in_the_last_6_months"
                    , "No_weight_gain_of_more_than_2_to_3_kg_in_the_last_2_months"
                    , "Exercise_function"
                    , "Nutrition")

ScriptSQl().upload("behavior.csv", "behavior")
ScriptSQl().create_sleep_table("year", "month", "day", "sleepHour")
ScriptSQl().upload("sleep_div.csv", "sleep")
ScriptSQl().create_ML_table("ML")
ScriptSQl().upload("ML.csv", "ML")

#Done
