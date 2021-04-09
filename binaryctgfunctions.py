"""
CTG Importtool POC
(c) 2021 ilionx

Versie: 0.1 (11-03-2021)
Auteur: R. Jonker, P. Groot

Importfuncties voor .CTG bestanden gebaseerd op de importfunctionaliteit uit de STVcalc applicatie
van Hans Wolf (UMC Amsterdam)
"""
from hixfunctions import format_HiX
import hixfunctions

def ReadBinaryCTG_Count(data):
    result = {}
    tel = 0
    count1 = 0
    count2 = 0
    countUtP = 0
    while tel < len(data):
        # kanaal 1
        b = bin(data[tel])
        b0 = '0b' + ('0000000000000000' + b[2:])[-16:]
        b = bin(data[tel + 1])
        b1 = '0b' + ('0000000000000000' + b[2:])[-16:]
        b = bin(data[tel + 2])
        b2 = '0b' + ('0000000000000000' + b[2:])[-16:]
        b = bin(data[tel + 3])
        b3 = '0b' + ('0000000000000000' + b[2:])[-16:]
        # kanaal 2
        b = bin(data[tel + 4])
        b4 = '0b' + ('0000000000000000' + b[2:])[-16:]
        b = bin(data[tel + 5])
        b5 = '0b' + ('0000000000000000' + b[2:])[-16:]
        b = bin(data[tel + 6])
        b6 = '0b' + ('0000000000000000' + b[2:])[-16:]
        b = bin(data[tel + 7])
        b7 = '0b' + ('0000000000000000' + b[2:])[-16:]
        if (int(b0[-10:-2], 2)) > 0 or (int(b1[-10:-2], 2)) > 0 or (int(b2[-10:-2], 2)) > 0 or (int(b3[-10:-2], 2)) > 0:
            count1 += 1
        if (int(b4[-10:-2], 2)) > 0 or (int(b5[-10:-2], 2)) > 0 or (int(b6[-10:-2], 2)) > 0 or (int(b7[-10:-2], 2)) > 0:
            count2 += 1
        # uterus tonus (TOCO)
        b = bin(data[tel + 9])
        b9 = '0b' + ('0000000000000000' + b[2:])[-16:]
        b = bin(data[tel + 10])
        b10 = '0b' + ('0000000000000000' + b[2:])[-16:]
        if (int(b9[-10:-2], 2)) >0 or (int(b10[-10:-2], 2)) >0:
            countUtP += 1
        tel += 12
    result['ch1'] = count1
    result['ch2'] = count2
    result['UtP'] = countUtP
    return result

def ReadBinaryCTG_HR_Fetus(data, channel, twins):
    """
    Uitlezen van kanaal 1 in .CTG bestand

    :param data:    Een lijst met waardes uit het CTG-bestand
    :param channel: Het inputkanaal
    :param twins:   Of er sprake is van een tweeling, bij een tweeling
                    wordt kanaal 2 hoogstwaarschijnlijk gebruikt voor de tweede foetus
    :return:        Een string met meetwaardes
    """
    tel = 0
    singleton = 0
    hr = []

    if channel == 2:
        singleton = 4

    while tel < len(data):
        hr.append(GetFetusHr(data, tel, singleton, twins))
        tel += 12

    return format_HiX(hr)

def ReadBinaryCTG_UtP(data):
    """
    Uitlezen van Uterus Tonus (Weeen) in CTG bestand

    :param data:    Een lijst met waardes uit het CTG-bestand
    :return:        Een string met meetwaardes
    """
    tel = 0
    utP = []

    while tel < len(data):
        res = []
        b = bin(data[tel + 9])
        b9 = '0b' + ('0000000000000000' + b[2:])[-16:]
        b = bin(data[tel + 10])
        b10 = '0b' + ('0000000000000000' + b[2:])[-16:]
        res.append(int(b9[-8:], 2))
        res.append(int(b10[-8:], 2))
        utP.append(res)
        tel += 12
    return format_HiX(utP)

def GetFetusHr(data, tel, singleton, twins):
    hr = []
    b0 = bin(data[tel + singleton])
    b0 = '0b' + ('0000000000000000' + b0[2:])[-16:]
    b1 = bin(data[tel + 1 + singleton])
    b1 = '0b' + ('0000000000000000' + b1[2:])[-16:]
    b2 = bin(data[tel + 2 + singleton])
    b2 = '0b' + ('0000000000000000' + b2[2:])[-16:]
    b3 = bin(data[tel + 3 + singleton])
    b3 = '0b' + ('0000000000000000' + b3[2:])[-16:]
    if twins == False and (int(b0[2:16]) + int(b1[2:16]) + int(b2[2:16]) + int(b3[2:16])) == 0:
        if singleton == 0:
            singleton = 4
        if singleton == 4:
            singleton = 0

        b0 = bin(data[tel + singleton])
        b0 = '0b' + ('0000000000000000' + b0[2:])[-16:]
        b1 = bin(data[tel + 1 + singleton])
        b1 = '0b' + ('0000000000000000' + b1[2:])[-16:]
        b2 = bin(data[tel + 2 + singleton])
        b2 = '0b' + ('0000000000000000' + b2[2:])[-16:]
        b3 = bin(data[tel + 3 + singleton])
        b3 = '0b' + ('0000000000000000' + b3[2:])[-16:]
    hr.append(int(b0[-10:-2], 2))
    hr.append(int(b1[-10:-2], 2))
    hr.append(int(b2[-10:-2], 2))
    hr.append(int(b3[-10:-2], 2))
    return hr

def ReadCTG(filePath):
    """
    Verwerking van .ctg bestanden, waaruit de verschillende CTG metingen worden geextraheerd.

    :param filePath:    Bestandslocatie van het te importen
    :return:
    """
    heartRateChannel_1 = []
    heartRateChannel_2 = []
    qualityChannel_1 = []
    qualityChannel_2 = []
    heartRateCountChannel_1 = 0
    heartRateCountChannel_2 = 0
    heartRateResultChannel_1 = ''
    heartRateResultChannel_2 = ''
    maternalheartRate=[]
    maternalquality = []
    maternalheartRateCount=0
    maternalheartRateResult=''
    uterusPressure = []
    uterusPressureCount = 0
    uterusPressureResult = ''
    print('Reading file',filePath)
    with open(filePath, 'br') as File:
        byte_content = File.read()
        data = [byte_content[i + 1] << 8 | byte_content[i] for i in range(0, len(byte_content), 2)]
        #data = [byte & 1 for byte in byte_content]
    File.close()
    
    mTel=0
    mCount1=0
    mCount2=0
    #To detect if channel 1 or 2 was connected to the US probe (should always be 1 in singletons)
    while mTel < len(data) and mCount1 < 100 and mCount2<100:
        mB = bin(data[mTel])
        mB1 = '0b' + ('0000000000000000' + mB[2:])[-16:]
        mB = bin(data[mTel + 4])
        mB2 = '0b' + ('0000000000000000' + mB[2:])[-16:]
        if (int(mB1[-10:-2], 2)) > 0:
            mCount1 += 1
        if (int(mB2[-10:-2], 2)) > 0:
            mCount2 +=1
        mTel += 12
    if mCount2 > 0:
        mSingle = 4
    elif mCount1 >0:
        mSingle = 1
    else:
        mSingle = -1
    

    tel = 0
    while tel < len(data):
        if mSingle==1:
            # Channel 1: byte 0 - 3
            # 4Hz (4 metingen per seconde)
            b = bin(data[tel])
            b0 = '0b' + ('0000000000000000' + b[2:])[-16:]
            i0 = int(b0[-10:-2], 2)
            q0 = int(b0[-13:-10], 2)
            b = bin(data[tel+1])
            b1 = '0b' + ('0000000000000000' + b[2:])[-16:]
            i1 = int(b1[-10:-2], 2)
            q1 = int(b1[-13:-10], 2)
            b = bin(data[tel+2])
            b2 = '0b' + ('0000000000000000' + b[2:])[-16:]
            i2 = int(b2[-10:-2], 2)
            q2 = int(b2[-13:-10], 2)
            b = bin(data[tel+3])
            b3 = '0b' + ('0000000000000000' + b[2:])[-16:]
            i3 = int(b3[-10:-2], 2)
            q3 = int(b3[-13:-10], 2)
            heartRateChannel_1.append(i0)
            heartRateChannel_1.append(i1)
            heartRateChannel_1.append(i2)
            heartRateChannel_1.append(i3)
            qualityChannel_1.append(q0)
            qualityChannel_1.append(q1)
            qualityChannel_1.append(q2)
            qualityChannel_1.append(q3)
            if i0 > 0 or i1 > 0 or i2 > 0 or i3 > 0:
                heartRateCountChannel_1 += 1
        elif mSingle==4:
            # Channel 1: byte 0 - 3
            # 4Hz (4 metingen per seconde)
            b = bin(data[tel])
            b0 = '0b' + ('0000000000000000' + b[2:])[-16:]
            i0 = int(b0[-10:-2], 2)
            q0 = int(b0[-13:-10], 2)
            b = bin(data[tel+1])
            b1 = '0b' + ('0000000000000000' + b[2:])[-16:]
            i1 = int(b1[-10:-2], 2)
            q1 = int(b1[-13:-10], 2)
            b = bin(data[tel+2])
            b2 = '0b' + ('0000000000000000' + b[2:])[-16:]
            i2 = int(b2[-10:-2], 2)
            q2 = int(b2[-13:-10], 2)
            b = bin(data[tel+3])
            b3 = '0b' + ('0000000000000000' + b[2:])[-16:]
            i3 = int(b3[-10:-2], 2)
            q3 = int(b3[-13:-10], 2)
            heartRateChannel_1.append(i0)
            heartRateChannel_1.append(i1)
            heartRateChannel_1.append(i2)
            heartRateChannel_1.append(i3)
            qualityChannel_1.append(q0)
            qualityChannel_1.append(q1)
            qualityChannel_1.append(q2)
            qualityChannel_1.append(q3)
            if i0 > 0 or i1 > 0 or i2 > 0 or i3 > 0:
                heartRateCountChannel_1 += 1
            # Channel 2: byte 4-7
            # 4Hz (4 metingen per seconde)
            b = bin(data[tel+4])
            b4 = '0b' + ('0000000000000000' + b[2:])[-16:]
            i4 = int(b4[-10:-2], 2) 
            q4 = int(b4[-13:-10], 2)
            b = bin(data[tel+5])
            b5 = '0b' + ('0000000000000000' + b[2:])[-16:]
            i5 = int(b5[-10:-2], 2)
            q5 = int(b5[-13:-10], 2)
            b = bin(data[tel+6])
            b6 = '0b' + ('0000000000000000' + b[2:])[-16:]
            i6 = int(b6[-10:-2], 2) 
            q6 = int(b6[-13:-10], 2)
            b = bin(data[tel+7])
            b7 = '0b' + ('0000000000000000' + b[2:])[-16:]
            i7 = int(b7[-10:-2], 2) 
            q7 = int(b7[-13:-10], 2)
            heartRateChannel_2.append(i4)
            heartRateChannel_2.append(i5)
            heartRateChannel_2.append(i6)
            heartRateChannel_2.append(i7)
            qualityChannel_2.append(q4)
            qualityChannel_2.append(q5)
            qualityChannel_2.append(q6)
            qualityChannel_2.append(q7)
            if i4 > 0 or i5 > 0 or i6 > 0 or i7 > 0:
                heartRateCountChannel_2 += 1
        else:
            continue
        #maternal Heart Rate
        # 2Hz (2 metingen per seconde)
        b = bin(data[tel+8])
        b8 = '0b' + ('0000000000000000' + b[2:])[-16:]
        i8 = int(b7[-10:-2], 2)
        q8 = int(b7[-13:-10], 2)
        b = bin(data[tel+9])
        b9 = '0b' + ('0000000000000000' + b[2:])[-16:]
        i9 = int(b7[-10:-2], 2)
        q9 = int(b7[-13:-10], 2)
        maternalheartRate.append(i8)
        maternalheartRate.append(i9)
        maternalquality.append(q8)
        maternalquality.append(q9)
        if i8 > 0 or i9 > 0 :
                maternalheartRateCount += 1
        # Uterus pressure (uterus tonus, weeeen, TOCO)
        # 2Hz (2 metingen per seconde)
        b = bin(data[tel + 10])
        b10 = '0b' + ('0000000000000000' + b[2:])[-16:]
        u0 = int(b10[-8:], 2)
        u1 = int(b10[-16:-8], 2)
        uterusPressure.append(u0)
        uterusPressure.append(u1)
        if u0 > 0 or u1 > 0:
            uterusPressureCount += 1
        tel += 12
    heartRateResultChannel_1 = hixfunctions.format_CTG_String(heartRateChannel_1)
    qualityResultChannel_1 = hixfunctions.format_CTG_String(qualityChannel_1)
    heartRateResultChannel_2 = hixfunctions.format_CTG_String(heartRateChannel_2)
    qualityResultChannel_2 = hixfunctions.format_CTG_String(qualityChannel_2)
    maternalheartRateResult = hixfunctions.format_CTG_String(maternalheartRate)
    maternalQualityResult = hixfunctions.format_CTG_String(maternalquality)
    uterusPressureResult = hixfunctions.format_CTG_String(uterusPressure)
    return heartRateResultChannel_1, heartRateCountChannel_1, qualityResultChannel_1, heartRateResultChannel_2, heartRateCountChannel_2, qualityResultChannel_2,\
            maternalheartRateResult, maternalheartRateCount, maternalQualityResult, uterusPressureResult, uterusPressureCount


def ReadDAT(filePath):
    """
    Verwerking van .dat bestanden, waaruit de verschillende CTG metingen worden geextraheerd.

    :param filePath:    Bestandslocatie van het te importen
    :return:
    """
    heartRateChannel_1 = []
    heartRateChannel_2 = []
    qualityChannel_1 = []
    qualityChannel_2 = []
    heartRateCountChannel_1 = 0
    heartRateCountChannel_2 = 0
    heartRateResultChannel_1 = ''
    heartRateResultChannel_2 = ''
    uterusPressure = []
    uterusPressureCount = 0
    uterusPressureResult = ''
    print('Reading file', filePath)
    with open(filePath, 'br') as File:
        byte_content = File.read()
        data = [byte_content[i + 1] << 8 | byte_content[i] for i in range(0, len(byte_content), 2)]
        # data = [byte & 1 for byte in byte_content]
    File.close()

    tel = 0
    while tel < len(data) - 12:
        # Channel 1: byte 0 - 3
        # 4Hz (4 metingen per seconde)
        b = bin(data[tel])
        b0 = '0b' + ('0000000000000000' + b[2:])[-16:]
        i0 = int(b0[-10:-2], 2)
        q0 = int(b0[-13:-10], 2)
        b = bin(data[tel] + 1)
        b1 = '0b' + ('0000000000000000' + b[2:])[-16:]
        i1 = int(b1[-10:-2], 2)
        q1 = int(b1[-13:-10], 2)
        b = bin(data[tel] + 2)
        b2 = '0b' + ('0000000000000000' + b[2:])[-16:]
        i2 = int(b2[-10:-2], 2)
        q2 = int(b2[-13:-10], 2)
        b = bin(data[tel] + 3)
        b3 = '0b' + ('0000000000000000' + b[2:])[-16:]
        i3 = int(b3[-10:-2], 2)
        q3 = int(b3[-13:-10], 2)
        heartRateChannel_1.append(i0)
        heartRateChannel_1.append(i1)
        heartRateChannel_1.append(i2)
        heartRateChannel_1.append(i3)
        qualityChannel_1.append(q0)
        qualityChannel_1.append(q1)
        qualityChannel_1.append(q2)
        qualityChannel_1.append(q3)
        if i0 > 0 or i1 > 0 or i2 > 0 or i3 > 0:
            heartRateCountChannel_1 += 1
        # Channel 2: byte 4-7
        # 4Hz (4 metingen per seconde)
        b = bin(data[tel] + 7)
        b4 = '0b' + ('0000000000000000' + b[2:])[-16:]
        i4 = int(b4[-10:-2], 2) - 1
        q4 = int(b4[-13:-10], 2)
        b = bin(data[tel] + 8)
        b5 = '0b' + ('0000000000000000' + b[2:])[-16:]
        i5 = int(b5[-10:-2], 2) - 1
        q5 = int(b5[-13:-10], 2)
        b = bin(data[tel] + 9)
        b6 = '0b' + ('0000000000000000' + b[2:])[-16:]
        i6 = int(b6[-10:-2], 2) - 1
        q6 = int(b6[-13:-10], 2)
        b = bin(data[tel] + 10)
        b7 = '0b' + ('0000000000000000' + b[2:])[-16:]
        i7 = int(b7[-10:-2], 2) - 1
        q7 = int(b7[-13:-10], 2)
        heartRateChannel_2.append(i4)
        heartRateChannel_2.append(i5)
        heartRateChannel_2.append(i6)
        heartRateChannel_2.append(i7)
        qualityChannel_2.append(q4)
        qualityChannel_2.append(q5)
        qualityChannel_2.append(q6)
        qualityChannel_2.append(q7)
        if i4 > 0 or i5 > 0 or i6 > 0 or i7 > 0:
            heartRateCountChannel_2 += 1
        # Uterus pressure (uterus tonus, weeeen, TOCO)
        # 2Hz (2 metingen per seconde)
        b = bin(data[tel + 5])
        b10 = '0b' + ('0000000000000000' + b[2:])[-16:]
        u0 = int(b10[-8:], 2)
        b = bin(data[tel + 6])
        b11 = '0b' + ('0000000000000000' + b[2:])[-16:]
        u1 = int(b11[-16:-8], 2)
        uterusPressure.append(u0)
        uterusPressure.append(u1)
        if u0 > 0 or u1 > 0:
            uterusPressureCount += 1
        tel += 12
    heartRateResultChannel_1 = hixfunctions.format_CTG_String(heartRateChannel_1)
    qualityResultChannel_1 = hixfunctions.format_CTG_String(qualityChannel_1)
    heartRateResultChannel_2 = hixfunctions.format_CTG_String(heartRateChannel_2)
    qualityResultChannel_2 = hixfunctions.format_CTG_String(qualityChannel_2)
    uterusPressureResult = hixfunctions.format_CTG_String(uterusPressure)
    return heartRateResultChannel_1, heartRateCountChannel_1, qualityResultChannel_1, heartRateResultChannel_2, heartRateCountChannel_2, qualityResultChannel_2, uterusPressureResult, uterusPressureCount
