# %%
import pandas as pd
import numpy as np
import glob
import os
from tqdm import tqdm

from sqlalchemy import create_engine
from sqlalchemy import text
from config import url_local_postgres


# %%
def dataframe_agents_and_companys():
    # Cargar todos los archivos JSON en un solo DataFrame
    file_paths=glob.glob("data/simem/972263/2025-*-*.json")

    columns={
        'Fecha': 'fecha',
        'CodigoSICAgente': 'agente',
        'NombreAgente': 'nombre_empresa',
        'ActividadAgente': 'nombre_actividad'
    }
    records=[]
    for file_path in tqdm(file_paths):
        df=pd.read_json(file_path)

        df=df[columns.keys()]
        df=df.rename(columns=columns)

        df['empresa'] = df['agente'].str[:3]
        df['actividad'] = df['agente'].str[-1]

        df['fecha'] = pd.to_datetime(df['fecha'], format='%Y-%m-%d')
        
        records.append(df)

    df=pd.concat(records, ignore_index=True)
    df= df.sort_values(by=['fecha', 'agente']).reset_index(drop=True)
    df=df.drop_duplicates(subset=['agente'], keep='last')


    return df

# %%
def dataframe_contracts():
    file_paths=glob.glob("data/simem/d31647/2025-*-*.json")


    columns={
        "Fecha": "fecha",
        "CodigoAgenteComprador": "agente",
        "TipoMercado": "nombre_tipo_mercado",
        "Cantidad": "despacho_kwh",
        "PPP": "precio_ponderado_$/kwh",
    }

    records=[]
    for file_path in tqdm(file_paths):
        df=pd.read_json(file_path)

        df=df[columns.keys()]
        df=df.rename(columns=columns)

        df['fecha'] = pd.to_datetime(df['fecha'], format='%Y-%m-%d')
        df=df[(df['despacho_kwh']>0 ) & (df['precio_ponderado_$/kwh']>0)]
        df['total_comprado_$'] = df['despacho_kwh'] * df['precio_ponderado_$/kwh']
        records.append(df)

    df=pd.concat(records, ignore_index=True)

    df["tipo_mercado"] = df["nombre_tipo_mercado"].map({'No Regulado': 'N', 'Regulado': 'R'})
    df= df.sort_values(by=['fecha', 'agente']).reset_index(drop=True)


    return df

# %%
df=dataframe_contracts()

df.columns.to_list()

# %%
def create_table_agents():
     # Crear la tabla en PostgreSQL
    df=dataframe_agents_and_companys()
    columns=['agente', 'empresa', 'actividad']
    df=df[columns]
    engine=create_engine(url_local_postgres)
    df.to_sql('agentes', engine, if_exists='replace', index=False)
    return df

# %%
def create_table_companies():
    df=dataframe_agents_and_companys()
    columns=['empresa', 'nombre_empresa']
    df=df[columns]
    df=df.drop_duplicates(subset=['empresa'], keep='last')
    df=df.sort_values(by=['empresa']).reset_index(drop=True)
    engine=create_engine(url_local_postgres)
    df.to_sql('empresas', engine, if_exists='replace', index=False)
    return df


# %%
def create_table_actividades():
    df=dataframe_agents_and_companys()
    columns=['actividad', 'nombre_actividad']
    df=df[columns]
    df=df.drop_duplicates(subset=['actividad'], keep='last')
    df=df.sort_values(by=['actividad']).reset_index(drop=True)
    engine=create_engine(url_local_postgres)
    df.to_sql('actividades', engine, if_exists='replace', index=False)
    return df

# %%
def create_table_contracts():
    df=dataframe_contracts()

    columns=[
        'fecha',
        'agente',
        'despacho_kwh',
        'precio_ponderado_$/kwh',
        'total_comprado_$',
        'tipo_mercado'
    ]
    df=df[columns]
     # Crear la tabla en PostgreSQL
    engine=create_engine(url_local_postgres)
    df.to_sql('contratos', engine, if_exists='replace', index=False)
    return df


# %%
def create_table_markets():
    df=dataframe_contracts()

    columns=[
        'tipo_mercado',
        'nombre_tipo_mercado'

    ]
    df=df[columns]
     # Crear la tabla en PostgreSQL
    df=df.drop_duplicates(subset=['tipo_mercado'], keep='last')
    engine=create_engine(url_local_postgres)
    df.to_sql('tipo_mercados', engine, if_exists='replace', index=False)
    return df

# %%
def create_primary_keys():
    engine=create_engine(url_local_postgres)

    with engine.connect() as conn:
        conn.execute(text("ALTER TABLE public.actividades ADD CONSTRAINT pk_actividad PRIMARY KEY (actividad);"))
        conn.execute(text("COMMIT;"))
        conn.execute(text("ALTER TABLE public.empresas ADD CONSTRAINT pk_empresa PRIMARY KEY (empresa);"))
        conn.execute(text("COMMIT;"))
        conn.execute(text("ALTER TABLE public.agentes ADD CONSTRAINT pk_agente PRIMARY KEY (agente);"))
        conn.execute(text("COMMIT;"))
        conn.execute(text("ALTER TABLE public.tipo_mercados ADD CONSTRAINT pk_tipo_mercado PRIMARY KEY (tipo_mercado);"))
        conn.execute(text("COMMIT;"))




# %%
# %%
def create_relationships():
    engine = create_engine(url_local_postgres)

    with engine.connect() as conn:
        # Relación entre agentes.actividad -> actividades.actividad
        conn.execute(text("""
            ALTER TABLE public.agentes 
            ADD CONSTRAINT fk_agente_actividad 
            FOREIGN KEY (actividad) REFERENCES public.actividades(actividad);
        """))
        conn.execute(text("COMMIT;"))

    with engine.connect() as conn:
        # Relación entre agentes.empresa -> actividades.empresa
        conn.execute(text("""
            ALTER TABLE public.agentes 
            ADD CONSTRAINT fk_agente_empresa 
            FOREIGN KEY (empresa) REFERENCES public.empresas(empresa);
        """))
        conn.execute(text("COMMIT;"))

    with engine.connect() as conn:
        # Relación entre contratos.agente -> agentes.agente
        conn.execute(text("""
            ALTER TABLE public.contratos 
            ADD CONSTRAINT fk_agente_agente 
            FOREIGN KEY (agente) REFERENCES public.agentes(agente);
        """))
        conn.execute(text("COMMIT;"))

    with engine.connect() as conn:
        # Relación entre contratos.tipo_mercado -> tipo_mercados.tipo_mercado
        conn.execute(text("""
            ALTER TABLE public.contratos 
            ADD CONSTRAINT fk_contrato_tipo_mercado 
            FOREIGN KEY (tipo_mercado) REFERENCES public.tipo_mercados(tipo_mercado);
        """))
        conn.execute(text("COMMIT;"))




# %%
def main():
    print("Creando tabla agentes...")
    df_agents=create_table_agents()
    print(df_agents.head())

    print("Creando tabla empresas...")
    df_companies=create_table_companies()
    print(df_companies.head())

    print("Creando tabla actividades...")
    df_activities=create_table_actividades()
    print(df_activities.head())


    print("Creando tabla tipo_mercados...")
    df_markets=create_table_markets()
    print(df_markets.head())

    print("Creando tabla contratos...")
    df_contracts=create_table_contracts()
    print(df_contracts.head())
    
    print("Creando llaves primarias...")
    create_primary_keys()
 
    print("Creando relaciones...")
    create_relationships()  
 
 

# %%
if __name__ == "__main__":
    main()


