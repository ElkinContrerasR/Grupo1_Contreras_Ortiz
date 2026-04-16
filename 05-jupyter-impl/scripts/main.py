from data_fake import generate_synthetic_data
from extractor import SpaceX

etl = SpaceX()

data = generate_synthetic_data(500)

# IMPORTANTE: agregar central
data["launches_central"] = etl.build_launches_central(data)

etl.cargar_en_bd(data)