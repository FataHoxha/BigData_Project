
import re

import spacy

nlp = spacy.load('en_core_web_sm')

# Function to parse the html code -> delete everithing in the middle of <>


#functions that requires regexp

def remove_html(raw_html):
    cleanr = re.compile('<.*?>')
    cleantext = re.sub(cleanr, '', raw_html)
    return cleantext
def remove_url(text):
    #text = re.sub(r'^https?:\/\/.*[\r\n]*', '', text, flags=re.MULTILINE)
    text = re.sub(r'http[s]?://(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+', '', text)
    return text
def remove_punctation(text):
    text = re.sub(r'[^\w\s]', ' ', text)
    text = str(text).replace('\t', ' ').replace('\n', '')
    return text

def remove_digits(text):
    text = re.sub("\d+", "", text)
    return text

def lower_case(text):
    text = text.lower()
    return text

def remove_user_link(text):
    text=re.sub("http://twitter.com/",'',text)
    return text

def remove_empty_space(text):
    text=re.sub(' +',' ',text)
    return text
#functions that requires NLP library

def remove_non_english(text):

    return text

def lemmatization(text):
    return text

def stop_words(text):
    return text

def remove_short_word(text):
    return text


def text_normalization(text):
    text_complete=""
    doc_comment = nlp(unicode(text))
    string_text =''
    #print "DOC COMMENT --->", doc_comment
    for token_comment in doc_comment:
		token_comment = token_comment.lemma_
		doc_token = nlp(token_comment)
		token_comm=""
		for token in doc_token:
			if (token.is_stop==False):
				if (str(token) != '-PRON-'):
					token_comm=str(token)
				if (len(token_comm)>2):
					string_text+=token_comm+" "
    text_complete+=string_text
    text = re.sub(' +', " ", text_complete)
    return text