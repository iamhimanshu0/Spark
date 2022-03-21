import random
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import warnings
warnings.filterwarnings("ignore")
# warnings.simplefilter('always')

sc = SparkContext(appName = "Text Cleaning")
strc = StreamingContext(sc, 3)


text_data = strc.socketTextStream("localhost", 8083)


# import re
# from nltk.corpus import stopwords
# stop_words = set(stopwords.words('english'))
# from nltk.stem import WordNetLemmatizer
# lemmatizer = WordNetLemmatizer()


# def clean_text(sentence):
#     sentence = sentence.lower()
#     sentence = re.sub("\s+"," ", sentence)
#     sentence = re.sub("\W"," ", sentence)
#     sentence = re.sub(r"http\S+", "", sentence)
#     sentence = ' '.join(word for word in sentence.split() if word not in stop_words)
#     sentence = [lemmatizer.lemmatize(token, "v") for token in sentence.split()]
#     sentence = " ".join(sentence)
#     return sentence.strip()

def clean_text(word):
    sentence = f"this is some random number {random.randint(1,10)} and word is {word}"
    return sentence


cleaned_text = text_data.map(lambda line: clean_text(line))
cleaned_text.pprint()

strc.start()
strc.awaitTermination()




# https://www.analyticsvidhya.com/blog/2021/06/real-time-data-streaming-using-apache-spark/
