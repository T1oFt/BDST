two_letter_codes_set = {'kk', 'ug', 'la', 'ng', 'om', 'st', 'ga', 'nn', 'pa', 'br', 'ak', 'ia', 'te', 'no', 'fo', 'sm', 'sa', 'hi', 'pl', 'lb', 'el', 'li', 'cu', 'rw', 'bn', 'ht', 'sd', 'ff', 'ba', 'ie', 'jv', 'ne', 'tn', 'ky', 'ee', 'rm', 'zh', 'cv', 'io', 'na', 'az', 'wa', 'av', 'dv', 'id', 'th', 'ku', 'zu', 'tg', 'ta', 'ii', 'co', 'gn', 'yi', 'mh', 'cr', 'sc', 'ik', 'ab', 'ce', 'to', 'mr', 'ko', 'fj', 'nd', 'tl', 'tt', 'oj', 'bo', 'ar', 'kw', 'pt', 'ty', 'sv', 'tr', 'si', 'gd', 'ro', 'xh', 'se', 'gu', 'lv', 'ss', 'hz', 'uz', 've', 'ca', 'mk', 'hr', 'ha', 'iu', 'ts', 'nl', 'tw', 'vi', 'za', 'oc', 'ti', 'ay', 'it', 'lu', 'be', 'lt', 'nr', 'rn', 'pi', 'fa', 'su', 'fr', 'bg', 'tk', 'ki', 'aa', 'is', 'so', 'he', 'bm', 'ml', 'gl', 'qu', 'da', 'os', 'kl', 'vo', 'ig', 'ae', 'kr', 'es', 'sw', 'yo', 'ny', 'de', 'lg', 'ch', 'ho', 'ur', 'my', 'kj', 'hy', 'kv', 'ln', 'sk', 'mn', 'sl', 'ka', 'hu', 'wo', 'et', 'nv', 'kg', 'lo', 'cy', 'cs', 'fy', 'eu', 'kn', 'ks', 'gv', 'as', 'dz', 'sg', 'mg', 'nb', 'bs', 'km', 'or', 'fi', 'am', 'ja', 'mt', 'bi', 'ps', 'ru', 'mi', 'sr', 'sn', 'en', 'af', 'ms', 'uk', 'an', 'eo', 'sq'}

three_letter_codes_set = {'bur', 'tgk', 'lao', 'kik', 'pan', 'tir', 'hat', 'uig', 'mah', 'rum', 'ces', 'uzb', 'ton', 'iii', 'fao', 'kir', 'alb', 'tha', 'ina', 'glv', 'nya', 'wln', 'iku', 'eus', 'asm', 'gle', 'ewe', 'mya', 'kal', 'epo', 'kur', 'aka', 'orm', 'run', 'khm', 'msa', 'nbl', 'amh', 'san', 'mal', 'kat', 'ita', 'spa', 'tur', 'fry', 'srd', 'smo', 'ava', 'wol', 'pol', 'ven', 'nld', 'her', 'nau', 'sot', 'nor', 'vol', 'bak', 'kin', 'ger', 'nde', 'tat', 'pus', 'wel', 'kor', 'hye', 'arg', 'snd', 'tgl', 'swa', 'dzo', 'kas', 'vie', 'sqi', 'baq', 'bre', 'yid', 'aym', 'kau', 'chi', 'mkd', 'afr', 'lav', 'lit', 'ori', 'sun', 'cre', 'nob', 'lim', 'glg', 'mri', 'slk', 'ndo', 'hun', 'ara', 'mlg', 'roh', 'mlt', 'ben', 'sag', 'dut', 'hau', 'aze', 'tsn', 'kan', 'aar', 'geo', 'cze', 'mao', 'lug', 'lin', 'nep', 'swe', 'tib', 'isl', 'fas', 'kom', 'jav', 'bis', 'ssw', 'ibo', 'sna', 'fij', 'bos', 'chv', 'rus', 'ipk', 'que', 'hrv', 'kon', 'lat', 'tuk', 'abk', 'slo', 'lub', 'zho', 'hmo', 'div', 'oci', 'kaz', 'bul', 'xho', 'dan', 'ltz', 'slv', 'bod', 'ukr', 'yor', 'oss', 'ave', 'mon', 'est', 'jpn', 'ile', 'twi', 'sin', 'srp', 'ful', 'ido', 'ind', 'pli', 'tso', 'che', 'cos', 'som', 'cor', 'guj', 'oji', 'sme', 'urd', 'zul', 'tah', 'fin', 'gla', 'ell', 'arm', 'kua', 'mar', 'per', 'ice', 'fra', 'deu', 'cha', 'tam', 'tel', 'chu', 'cat', 'eng', 'hin', 'mac', 'fre', 'bam', 'heb', 'gre', 'grn', 'may', 'por', 'ron', 'nav', 'zha', 'cym', 'nno', 'bel'}

language_codes = two_letter_codes_set | three_letter_codes_set

libraries = {
    "PyTorch", "TensorFlow", "JAX", "Safetensors", "Transformers", "PEFT", "TensorBoard", "GGUF", "Diffusers", "ONNX", "stable-baselines3", "sentence-transformers", "ml-agents", "MLX", "TF-Keras", "Keras", "Adapters", "setfit", "timm", "Transformers.js", "sample-factory", "Joblib", "OpenVINO", "Flair", "fastai", "ESPnet", "BERTopic", "spaCy", "NeMo", "LiteRT", "Core ML", "OpenCLIP", "Rust", "Scikit-learn", "fastText", "KerasHub", "Asteroid", "speechbrain", "AllenNLP", "llamafile", "Fairseq", "PaddlePaddle", "Stanza", "Habana", "PaddleOCR", "Graphcore", "pyannote.audio", "SpanMarker", "paddlenlp", "unity-sentis", "DDUF", "univa"
}

multimodal_tasks = {
    "Audio-Text-to-Text",
    "Image-Text-to-Text",
    "Visual Question Answering",
    "Document Question Answering",
    "Video-Text-to-Text",
    "Visual Document Retrieval",
    "Any-to-Any",
}

computer_vision_tasks = {
    "Depth Estimation",
    "Image Classification",
    "Object Detection",
    "Image Segmentation",
    "Text-to-Image",
    "Image-to-Text",
    "Image-to-Image",
    "Image-to-Video",
    "Unconditional Image Generation",
    "Video Classification",
    "Text-to-Video",
    "Zero-Shot Image Classification",
    "Mask Generation",
    "Zero-Shot Object Detection",
    "Text-to-3D",
    "Image-to-3D",
    "Image Feature Extraction",
    "Keypoint Detection",
    "Video-to-Video",
}

nlp_tasks = {
    "Text Classification",
    "Token Classification",
    "Table Question Answering",
    "Question Answering",
    "Zero-Shot Classification",
    "Translation",
    "Summarization",
    "Feature Extraction",
    "Text Generation",
    "Fill-Mask",
    "Sentence Similarity",
    "Text Ranking",
}

audio_tasks = {
    "Text-to-Speech",
    "Text-to-Audio",
    "Automatic Speech Recognition",
    "Audio-to-Audio",
    "Audio Classification",
    "Voice Activity Detection",
}

tabular_tasks = {
    "Tabular Classification",
    "Tabular Regression",
    "Time Series Forecasting",
}

rl_tasks = {
    "Reinforcement Learning",
    "Robotics",
}

tasks = multimodal_tasks | computer_vision_tasks | nlp_tasks | audio_tasks | tabular_tasks | rl_tasks