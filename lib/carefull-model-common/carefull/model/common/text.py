import re


def normalize_transaction_name(document):
    # Remove all the special characters
    document = re.sub(r'\W', ' ', str(document))
    # remove all single characters
    document = re.sub(r'\s+[a-zA-Z]\s+', ' ', document)
    # Remove single characters from the start
    document = re.sub(r'\^[a-zA-Z]\s+', ' ', document)
    # Remove MM/dd
    document = re.sub(r'(^|\s)(0[1-9]|1[012])[- /.](0[1-9]|[12][0-9]|3[01])($|\s)', ' ', document)
    # remove _
    document = document.replace("_", " ")
    # Substituting multiple spaces with single space
    document = re.sub(r'\s+', ' ', document, flags=re.I)
    # Converting to Lowercase
    document = document.lower()
    # remove masc of XXX
    tokens = document.split()
    tokens = [word for word in tokens if not all(char in ['x'] for char in word)]
    preprocessed_text = ' '.join(tokens)
    return preprocessed_text

