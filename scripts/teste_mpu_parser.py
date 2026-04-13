

from vinea.mpu_parser import MPUParser


cliente = MPUParser()

df = cliente.ler_mpu_json("data/teste_mpu.json")


df1 = cliente.mpu_para_df_principal(df)


