import textblob
import spacy
from transformers import pipeline
from typing import List, Dict, Any

nlp = spacy.load("en_core_web_sm")
emo_classifier = pipeline("text-classification", model="SamLowe/roberta-base-go_emotions", top_k=1)


class NLPProcessor:
    @staticmethod
    def analyze_sentiment(text: str) -> str:
        """Determines sentiment polarity (positive, neutral, negative)."""
        polarity = textblob.TextBlob(text).sentiment.polarity
        if polarity > 0.1:
            return "positive"
        elif polarity < -0.1:
            return "negative"
        return "neutral"
    
    @staticmethod
    def detect_emotion(text: str) -> str:
        """Uses a Hugging Face transformer model to classify emotion."""
        try:
            predictions = emo_classifier(text)
            return predictions[0][0]['label']
        except Exception as e:
            print(f"Emotion classification error: {e}")
            return "unknown"
    
    @staticmethod
    def extract_topics(text: str) -> List[str]:
        """Extracts important topic keywords using NLP."""
        doc = nlp(text)
        topics = [token.lemma_ for token in doc if token.pos_ in {"NOUN", "PROPN"}]
        return list(set(topics))
