import logging
import time
from dataclasses import dataclass, field

from carefull.model.common.binary_models import BinaryModelRepo, BinaryModelType
from carefull.model.common.text import normalize_transaction_name
from fuzzywuzzy import fuzz
from simhash import Simhash, SimhashIndex


SIMHASH_FINGERPRINT_DIMENSION = 32
SIMHASH_TOLERANCE = 3
SIMILARITY_THRESHOLD = 80
LEN_SHINGLE = 3

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.getLogger("simhash").setLevel(level=logging.ERROR)
logger = logging.getLogger()

MODEL_INFO = {}


@dataclass
class NormalizedTxName:
    id: int
    name: str


@dataclass()
class NameVendor:
    name: str
    vendor_id: int
    score: int = field(init=False)


class NameVendorRepo:
    BY_ID = "select normalize_name, vendor_id from normalize_tx_name_vendor where id in %s"

    def __init__(self, connection) -> None:
        super().__init__()
        self.connection = connection

    def find_by_id_list(self, id_list):
        cursor = self.connection.cursor()
        cursor.execute(self.BY_ID, (tuple(id_list),))
        return [NameVendor(*x) for x in cursor.fetchall()]


def normalized_tx_name_iterator(connection):
    with connection.cursor(name='normalizedTxName') as cursor:
        cursor.itersize = 100000
        query = 'SELECT id, normalize_name FROM normalize_tx_name_vendor'
        cursor.execute(query)
        for row in cursor:
            yield NormalizedTxName(*row)


def get_features(s, width=3):
    return [s[i:i + width] for i in range(max(len(s) - width + 1, 1))]


class SimHashModelGenerator:
    def __init__(self, normalized_name_iter, simhash_f=SIMHASH_FINGERPRINT_DIMENSION, len_shingles=LEN_SHINGLE,
                 simhash_tolerance=SIMHASH_TOLERANCE, feature_generator=get_features):
        self.normalized_name_iter = normalized_name_iter
        self.feature_generator = feature_generator
        self.simhash_tolerance = simhash_tolerance
        self.len_shingles = len_shingles
        self.simhash_f = simhash_f

    def generate(self):
        simhash_list = []
        i = 0
        for normalized_name in self.normalized_name_iter:
            i += 1
            features = self.feature_generator(normalized_name.name, self.len_shingles)
            simhash_list.append((normalized_name.id, Simhash(features, f=self.simhash_f)))
            if i % 50000 == 0:
                logger.info(f'{i} records processed')
        logger.info(f'{i} records processed')
        simhash_index = SimhashIndex(simhash_list, k=self.simhash_tolerance, f=self.simhash_f)
        return {"SIMHASH_F": self.simhash_f, "LEN_SHINGLES": self.len_shingles, "SIMHASH_INDEX": simhash_index}


class VendorModel:

    def __init__(self, model_info, repo, name_normalizer=normalize_transaction_name, feature_generator=get_features,
                 threshold=SIMILARITY_THRESHOLD):
        self.repo = repo
        self.simhash_index = model_info["SIMHASH_INDEX"]
        self.simhash_fingerprints = model_info["SIMHASH_F"]
        self.shingles_len = model_info["LEN_SHINGLES"]
        self.threshold = threshold
        self.name_normalizer = name_normalizer
        self.feature_generator = feature_generator

    def extract_vendor(self, tx_name, verbose=False):
        if verbose:
            start_time = time.time()
        cleaned_description = self.name_normalizer(tx_name)
        f = self.feature_generator(cleaned_description)
        simhash = Simhash(f, f=self.simhash_fingerprints)
        near_dups = [x for x in self.simhash_index.get_near_dups(simhash)]
        if not near_dups:
            if verbose:
                logger.info("No near duplicates")
            return None
        candidates = self.repo.find_by_id_list(near_dups)
        winner = None
        for x in candidates:
            if x.name == cleaned_description:
                x.score = 100
                winner = x
                break
            x.score = fuzz.partial_ratio(x.name, cleaned_description)
        if not winner:
            candidates = sorted(candidates, key=lambda x: x.score, reverse=True)
            winner = candidates[0]
        if verbose:
            logger.info(f"{cleaned_description} - {winner} - Elapsed {time.time() - start_time}")
        return int(winner.vendor_id) if winner.score >= self.threshold else None


def load_vendor_model(local=False):
    if 'model' not in MODEL_INFO:
        model = BinaryModelRepo().load(BinaryModelType.VENDOR, local)
        MODEL_INFO['model'] = model
    return MODEL_INFO['model']


def regenerate_model(connection, local=False):
    start_time = time.time()
    normalized_name_iter = normalized_tx_name_iterator(connection)
    gene = SimHashModelGenerator(normalized_name_iter=normalized_name_iter)
    simhash_model = gene.generate()
    repo = BinaryModelRepo()
    repo.save(simhash_model, BinaryModelType.VENDOR, local)
    connection.close()
    logger.info(f"--- {(time.time() - start_time)} seconds ---")

