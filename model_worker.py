# This will "read in" the output configuration created by train.py
# To run in production 24/7
import tensorflow as tf
from transformers import BertTokenizer, TFBertForSequenceClassification

# https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs-example-sending-receiving-msgs.html

tokenizer = BertTokenizer.from_pretrained("fine_tuned_model_tokenizer")
model = TFBertForSequenceClassification.from_pretrained("fine_tuned_model")

def model_worker(text_input_arr):
    try:
        print('model_worker running...')
        tf_batch = tokenizer(text_input_arr, max_length=128, padding=True, truncation=True, return_tensors='tf')
        tf_outputs = model(tf_batch)
        tf_predictions = tf.nn.softmax(tf_outputs[0], axis=-1)
        results = tf.argmax(tf_predictions, axis=1)
        numpy_arr = results.numpy()
        converted_value = getattr(numpy_arr, "tolist", lambda value: value)()
        # attempt mapping 0 (negative) to -1, so if "most posts are negative" we don't lose magnitude of negative sentiment
        # vs summing 0s - it's still 0
        converted_value = map(lambda x: 1 if x == 1 else -1,converted_value)
        return list(converted_value)
    except Exception as model_error:
        print('model_worker failed:', model_error)
        raise RuntimeError('Failed to parse input in model_worker:') from model_error
