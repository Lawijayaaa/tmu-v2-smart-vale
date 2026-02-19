import mysql.connector

class DataStream:
    def __init__(self):
        self.conn = mysql.connector.connect(
            host="localhost",
            user="client",
            password="raspi",
            database="iot_trafo_client"
        )
        self.last_data_id = None

    def get_latest_values(self):
        self.conn.commit()
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT *
            FROM reading_data
            ORDER BY data_id DESC
            LIMIT 1
        """)
        row = cursor.fetchone()
        cursor.close()
        if row:
            return row
        return None

    def get_status(self):
        self.conn.commit()
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT *
            FROM transformer_status
            WHERE trafoId = 1
        """)
        row = cursor.fetchone()
        cursor.close()
        result = list(row)
        result.pop(0)
        print(result)
        return result

    def get_autoscroll(self):
        cursor = self.conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT intervalDelay
            FROM transformer_data   
            WHERE trafoId = 1
        """)
        row = cursor.fetchone()
        cursor.close()
        if row and row["intervalDelay"] is not None:
            return row["intervalDelay"]
        return 0
    
    def get_pages(self, textPropVal, textPropStat, colorPropStat, blinkPropStat, page):
        textPropVal.insert(0, 0)
        textPropStat.insert(0, 0)
        colorPropStat.insert(0, 0)
        blinkPropStat.insert(0, 0)
        #print(textPropStat)
        #print(colorPropStat)
        #print(blinkPropStat)
        pages = {0: ([textPropVal[1], False, False],
                     [textPropVal[5], False, False],
                     [textPropVal[6], False, False],
                     [textPropVal[11], False, False],
                     [textPropVal[9], False, False],
                     [textPropVal[41], False, False],
                     [textPropVal[46], False, False],
                     [textPropVal[45], False, False]),
                 1: ([textPropVal[1], False, False],
                     [textPropVal[2], False, False],
                     [textPropVal[3], False, False],
                     [textPropVal[4], False, False],
                     [textPropVal[7], False, False],
                     [textPropVal[22], False, False],
                     [textPropVal[26], False, False],
                     [textPropVal[30], False, False]),
                 2: ([textPropVal[1], False, False],
                     [textPropVal[55], False, False],
                     [textPropVal[56], False, False],
                     [textPropVal[57], False, False],
                     [textPropVal[35], False, False],
                     [textPropVal[8], False, False],
                     [textPropVal[10], False, False],
                     [textPropVal[12], False, False]),
                 3: ([textPropVal[1], False, False],
                     [textPropVal[19], False, False],
                     [textPropVal[20], False, False],
                     [textPropVal[21], False, False],
                     [textPropVal[23], False, False],
                     [textPropVal[24], False, False],
                     [textPropVal[25], False, False],
                     [textPropVal[36], False, False]),
                 4: ([textPropVal[1], False, False],
                     [textPropVal[27], False, False],
                     [textPropVal[28], False, False],
                     [textPropVal[29], False, False],
                     [textPropVal[31], False, False],
                     [textPropVal[32], False, False],
                     [textPropVal[33], False, False],
                     [textPropVal[34], False, False]),
                 5: ([textPropVal[1], False, False],
                     [textPropVal[13], False, False],
                     [textPropVal[14], False, False],
                     [textPropVal[15], False, False],
                     [textPropVal[16], False, False],
                     [textPropVal[17], False, False],
                     [textPropVal[18], False, False],
                     [textPropVal[37], False, False]),
                 6: ([textPropVal[1], False, False],
                     [textPropVal[47], False, False],
                     [textPropVal[48], False, False],
                     [textPropVal[49], False, False],
                     [textPropVal[50], False, False],
                     [textPropVal[51], False, False],
                     [textPropVal[52], False, False],
                     [textPropVal[53], False, False]),
                 7: ([textPropVal[1], False, False],
                     [textPropVal[38], False, False],
                     [textPropVal[39], False, False],
                     [textPropVal[40], False, False],
                     [textPropVal[42], False, False],
                     [textPropVal[43], False, False],
                     [textPropVal[44], False, False],
                     [textPropVal[54], False, False]),
                 8: ([textPropStat[8], colorPropStat[8], blinkPropStat[8]],
                     [textPropStat[9], colorPropStat[9], blinkPropStat[9]],
                     [textPropStat[10], colorPropStat[10], blinkPropStat[10]],
                     [textPropStat[11], colorPropStat[11], blinkPropStat[11]],
                     [textPropStat[12], colorPropStat[12], blinkPropStat[12]],
                     [textPropStat[13], colorPropStat[13], blinkPropStat[13]],
                     [textPropStat[14], colorPropStat[14], blinkPropStat[14]],
                     [textPropStat[15], colorPropStat[15], blinkPropStat[15]]),
                 9: ([textPropStat[1], colorPropStat[1], blinkPropStat[1]],
                     [textPropStat[2], colorPropStat[2], blinkPropStat[2]],
                     [textPropStat[3], colorPropStat[3], blinkPropStat[3]],
                     [textPropStat[4], colorPropStat[4], blinkPropStat[4]],
                     [textPropStat[5], colorPropStat[5], blinkPropStat[5]],
                     [textPropStat[6], colorPropStat[6], blinkPropStat[6]],
                     [textPropStat[7], colorPropStat[7], blinkPropStat[7]],
                     [textPropStat[24], colorPropStat[24], blinkPropStat[24]]),
                 10: ([textPropStat[16], colorPropStat[16], blinkPropStat[16]],
                     [textPropStat[17], colorPropStat[17], blinkPropStat[17]],
                     [textPropStat[18], colorPropStat[18], blinkPropStat[18]],
                     [textPropStat[19], colorPropStat[19], blinkPropStat[19]],
                     [textPropStat[20], colorPropStat[20], blinkPropStat[20]],
                     [textPropStat[21], colorPropStat[21], blinkPropStat[21]],
                     [textPropStat[22], colorPropStat[22], blinkPropStat[22]],
                     [textPropStat[23], colorPropStat[23], blinkPropStat[23]]),
                 11: ([textPropStat[24], colorPropStat[24], blinkPropStat[24]],
                     [textPropStat[25], colorPropStat[25], blinkPropStat[25]],
                     [textPropStat[26], colorPropStat[26], blinkPropStat[26]],
                     [textPropStat[27], colorPropStat[27], blinkPropStat[27]],
                     [textPropStat[28], colorPropStat[28], blinkPropStat[28]],
                     [" ", False, False],
                     [" ", False, False],
                     [" ", False, False])
                     }
        result = pages.get(page, pages[0])
        return result

    def get_snapshot(self, page = 0):
        values = self.get_latest_values()
        if not values or len(values) < 58:
            return None
        status = self.get_status()

        keyVal = ["Timestamp : ", 
                         "Voltage U-N : ", "Voltage V-N : ", "Voltage W-N : ",
                         "Voltage U-V : ", "Voltage V-W : ", "Voltage U-W : ",
                         "Current U : ", "Current V : ", "Current W : ", 
                         "Total Current : ", "Neutral Current : ",
                         "THDv Phase U : ", "THDv Phase V : ", "THDv Phase W : ",
                         "THDi Phase U : ", "THDi Phase V : ", "THDi Phase W : ",
                         "Active Power Pu : ", "Active Power Pv : ", "Active Power Pw : ", "Active Power Total : ",
                         "Reactive Power Qu : ", "Reactive Power Qv : ", "Reactive Power Qw : ", "Reactive Power Total : ",
                         "Apparent Power Su : ", "Apparent Power Sv : ", "Apparent Power Sw : ", "Apparent Power Total : ",
                         "Power Factor U : ", "Power Factor V : ", "Power Factor W : ", "Power Factor Total : ",
                         "Frequency : ", "Energy kWh : ", "Energy kVARh : ", 
                         "Busbar Temp U : ", "Busbar Temp V : ", "Busbar Temp W : ",
                         "Oil Temperature : ", "Winding Temp U : ", "Winding Temp V : ", "Winding Temp W : ",
                         "Tank Pressure : ", "Oil Level : ", "K-Rated U : ", "Derating U : ", 
                         "K-Rated V : ", "Derating V : ",  "K-Rated W : ", "Derating W : ",
                         "H2 Level (ppm) : ", "Moisture Level (ppm) : ",
                         "Voltage Unbalance UV : ", "Voltage Unbalance VW : ", "Voltage Unbalance UW : "]
        textPropVal = [" "]*58
        for i, key in enumerate(keyVal):
            textPropVal[i] = key + str(values[i+1])

        textPropStat = [" "]*28
        keyStat = ["Voltage U-V : ", "Voltage V-W : ", "Voltage U-W : ", 
               "Current U : ", "Current V : ", "Current W : ", "Neutral Current : ",
               "THDv U : ", "THDv V : ", "THDv W : ", "THDi U : ", "THDi V : ", "THDi W : ", 
               "Total Power Factor : ", "Frequency : ", 
               "Busbar Temp. U : ", "Busbar Temp. V : ", "Busbar Temp. W : ", "Oil Temp. : ",
               "Winding Temp. U : ", "Winding Temp. V : ", "Winding Temp. W : ", "Tank Pressure : ",
               "H2 Level (ppm) : ", "Moisture Level (ppm) : ",
               "Unballance Voltage U-V : ", "Unballance Voltage V-W : ", "Unballance Voltage U-W : "]
        colorPropStat, blinkPropStat = [False]*28, [False]*28
        for i, stat in enumerate(status[:28]):
            if not status:
                return self.get_pages(textPropVal, textPropStat, colorPropStat, blinkPropStat, page)
            if stat == 1 or stat == 5:
                colorPropStat[i] = True
                blinkPropStat[i] = True
                if stat == 5:
                    textPropStat[i] = keyStat[i] + "Extreme High"
                elif stat == 1:
                    textPropStat[i] = keyStat[i] + "Extreme Low"
            elif stat == 2 or stat == 4:
                colorPropStat[i] = True
                blinkPropStat[i] = False
                if stat == 4:
                    textPropStat[i] = keyStat[i] + "High"
                elif stat == 2:
                    textPropStat[i] = keyStat[i] + "Low"
            elif stat == 3:
                colorPropStat[i] = False
                blinkPropStat[i] = False
                textPropStat[i] = keyStat[i] + "Normal"

        return self.get_pages(textPropVal, textPropStat, colorPropStat, blinkPropStat, page)
