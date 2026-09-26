# OceanWatch Analytics — entrega 1

**MINE 4213 · Soluciones Intensivas en Datos · 2026-20 · Proyecto final, Entrega 1**
Plataforma: **Databricks Free Edition** (serverless, Unity Catalog, Photon)

> Integrantes: Santiago Rodriguez Cruz

Somos el equipo de datos de **OceanWatch Analytics**. En esta entrega exploramos una semana de posiciones AIS de aguas de EE. UU. publicadas por NOAA (**1 al 7 de junio de 2023**: 7 archivos, ~2,3 GB comprimidos, **60.533.559 posiciones**), respondemos las cinco preguntas del negocio y dejamos los datos almacenados de forma óptima para un propósito de consulta concreto.

---

## 1. Cómo ejecutar

1. En Databricks Free Edition, importar la carpeta `notebooks/`: Workspace → *Import* → cada archivo `.py`, que se importa como notebook.
2. Ejecutar los notebooks **en orden** con cómputo *Serverless*. `00_config` no se ejecuta solo,los demás lo cargan con `%run ./00_config`.

| Notebook | Requisito | Qué hace | Tiempo aprox. (serverless) |
|---|---|---|---|
| `00_config` | — | Nombres de UC, URLs oficiales, esquema explícito, umbrales y utilidades (haversine, clases de MMSI, regiones, medición de archivos y bytes) | — |
| `01_ingesta` | 1 (15 %) | Crea el catálogo y los esquemas; descarga los 7 zip con reintentos y verificación de integridad; descomprime en el Volume; lee con `StructType`; carga la tabla base Delta; carga los datos de referencia | ~4 min |
| `02_exploracion_calidad` | 2 (20 %) | Perfil (volumen, nulos, tipos, esloras) y diagnóstico de calidad en 34 reglas persistidas en `analytics.dq_findings` | ~4 min |
| `03_preguntas_negocio` | 3 (25 %) | Preguntas a–e con planes de ejecución y justificación | ~8 min |
| `04_almacenamiento_optimo` | 4 (25 %) | Laboratorio CSV / Parquet / Delta × layout, con archivos y bytes leídos y el efecto de `OPTIMIZE`; tabla de servicio | ~15 min |
| `05_gobernanza` | 5 (10 %) | Comentarios, propiedades, etiquetas y evidencia en `information_schema` | ~4 min |

`01_ingesta` es **idempotente**: si los zip ya están en el Volume y pasan la verificación CRC, no se vuelven a descargar.

## 2. Arquitectura en Unity Catalog

```
oceanwatch                              catálogo del proyecto
├── raw          landing (VOLUME): ais_zip/, ais_csv/, ref/  ·  ingest_manifest
├── ref          vessel_type_catalog  ·  world_port_index
├── analytics    ais_positions (base) · ais_positions_serving (optimizada) · dq_findings
│                perfil_* · dim_buque_semana · q_* (respuestas e intermedios)
└── storage_lab  files (VOLUME, Parquet V1/V2) · pos_delta_* (V3–V5) · resultados_*
```

## 3. Fuentes (citadas)

- **AIS Vessel Traffic**, NOAA Office for Coastal Management / BOEM, *Marine Cadastre*: <https://hub.marinecadastre.gov/pages/vesseltraffic>. Archivos diarios en `https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/AIS_2023_06_0{1..7}.zip`.
- **Diccionario de datos AIS** (Marine Cadastre): <https://coast.noaa.gov/data/marinecadastre/ais/data-dictionary.pdf>.
- **AIS Vessel Type and Group Codes** (NOAA/USCG/BOEM, 2018-05-23): <https://coast.noaa.gov/data/marinecadastre/ais/VesselTypeCodes2018.pdf>.
- **World Port Index**, NGA Pub. 150 (CSV `UpdatedPub150.csv`): <https://msi.nga.mil/Publications/WPI>.
- ITU-R **M.1371** (mensajes AIS y valores "no disponible") e ITU-R **M.585** (formato del MMSI).

## 4. Resultados principales

### 4.1 Ingesta (requisito 1)
- Se descargaron los 7 zip de `coast.noaa.gov` dentro del notebook: **2,36 GB comprimidos** y **6,48 GB de CSV**. Cada descarga funcionó al primer intento y la descarga completa tomó unos 2 minutos.
- La integridad se verifica en tres niveles:
  - bytes recibidos = `Content-Length`;
  - CRC-32 de cada miembro del zip (`zipfile.testzip`);
  - SHA-256 registrado en `raw.ingest_manifest`.
- Los CSV se leen con un `StructType` de 17 columnas y `_corrupt_record`. Antes se valida el encabezado de cada archivo y se comprueba que ningún MMSI tenga ceros a la izquierda ni caracteres no numéricos, lo que justifica guardarlo como `LONG`.
- **Completitud end-to-end**: las filas cargadas por archivo coinciden exactamente con las líneas del CSV menos el encabezado. Son **60.533.559 filas y 0 corruptas**.

### 4.2 Exploración y calidad (requisito 2)
- Entre 8,0 y 9,1 M de posiciones y entre 19,6 y 21,2 mil MMSI por día; **31.871 MMSI** en la semana. La clase B aporta el 33 % de las posiciones.
- El tráfico lo dominan **Tug Tow** (30,7 % de las posiciones con 3.958 buques) y **recreo/vela** (30,3 % con 17.177 buques).
- **34 reglas de calidad** en `analytics.dq_findings`, con condición SQL, severidad y tratamiento propuesto para la Entrega 2. Los hallazgos clave:
  - **saltos imposibles**: 0,05 % de los segmentos que acumulan el **83 % de la distancia bruta**;
  - **301 MMSI que no son buques**;
  - centinelas del estándar tomados como valores: `heading = 511` (55 %), `cog = 360` (17 %), `sog = 102,3` (160 mil filas);
  - **1,36 M de filas con IMO inválido**;
  - 1.388 duplicados exactos y 284 llaves `(mmsi, segundo)` en conflicto;
  - 4 posiciones fuera del área de cobertura de NOAA (una a 89,7° de latitud), válidas en rango pero imposibles para la red de receptores.
- También se documenta lo que **no** es un problema: 0 filas corruptas, 0 coordenadas fuera de rango, 0 inconsistencias de atributos estáticos y nulos estructurales en la clase B.

### 4.3 Preguntas de negocio (requisito 3)
| # | Respuesta |
|---|---|
| a | Entre **19.551 y 21.007** buques distintos por día. `approx_count_distinct` con `rsd` 0,05 **subestimó todos los días** (hasta −10,1 %); con `rsd` 0,01 quedó dentro de ±1 %. Plan: el exacto necesita 2 shuffles y 4 agregaciones; el aproximado, 1 shuffle de *sketches*. En producción: aproximado con `rsd` 0,01 para tableros y exacto para cifras oficiales. |
| b | Top por posiciones: **31 Towing** (16,6 M; 27 %), **37 Pleasure Craft** (14,5 M; 24 %), 60 Passenger, 30 Fishing, 36 Sailing, 90 Other, 70 Cargo, 52 Tug, 80 Tanker y 57. SOG media en navegación: carga 11,3 kn, pasajeros 11,0, tanqueros 10,2 y remolque 5,6. Se agrega antes del join y el catálogo va por *broadcast*. |
| c | Tras filtrar saltos imposibles, el top 10 lo forman buques oceánicos: cruceros (EMERALD PRINCESS, HARMONY OF THE SEAS, …) y ro-ro/contenedores (JEAN ANNE, MATSONIA, …), con ~4.000–4.500 km a 13–15 kn. El n.º 1, un remolcador (5.771 km a 18,6 kn), queda marcado como sospechoso para la Entrega 2. Sin filtro, el "ganador" tendría 7,9 M km. El plan muestra un solo shuffle `hashpartitioning(mmsi)` que comparten la deduplicación, la ventana y la suma. |
| d | Las 10 celdas H3 r8 más densas: Seattle (3), San Diego (2), Bellingham, Marina del Rey, Ventura, el canal Sabine–Neches y Port Everglades. **5 de 10 están a ≤ 5 km de un puerto WPI**. H3 nativo: el plan no tiene `BatchEvalPython` y reporta *fully supported by Photon*. |
| e | **40,0 %** de los 31.570 buques transmitió los 7 días y **18,5 %** apareció un solo día. Esos visitantes están en un 53 % en el Atlántico, 25 % en el Pacífico y 10 % en el Golfo, y el **66 % son embarcaciones de recreo clase B**. |

### 4.4 Almacenamiento óptimo (requisito 4)
Propósito: *la consulta diaria del operador portuario*, que filtra una fecha y una zona lat/lon. Cifras de la ejecución final del notebook 04 (Q1 = Houston/Galveston el 5 de junio; Q2 = Los Ángeles/Long Beach el 2 de junio):

| Variante | En disco (archivos) | Q1: archivos · MB leídos | Q2: archivos · MB leídos |
|---|---|---|---|
| V0 CSV | 6,0 GB (7) | 7 · 6.182 | 7 · 6.182 |
| V1 Parquet (snappy) | 1,8 GB (12) | 12 · 1.818 | 12 · 1.818 |
| V2 Parquet `partitionBy(event_date)` | 1,8 GB (28) | 4 · 257 | 3 · 272 |
| V3 Delta `PARTITIONED BY (event_date)` (zstd) | 1,3 GB (29) | 4 · 193 | 4 · 205 |
| **V4 Delta `CLUSTER BY (event_date, lat, lon)`** | **1,2 GB (28)** | **1 · 42** | **1 · 65** |
| V5 Delta en 28 micro-lotes | 1,2 GB (448) | 64 · 165 | 64 · 179 |
| V5 + `OPTIMIZE` (compactación) | 903 MB (19) | 4 · 190 | 4 · 192 |
| V5 + `OPTIMIZE ZORDER BY (event_date, lat, lon)` | 866 MB (14) | 1 · 50 | 1 · 63 |

- **Decisión**: Delta administrada en UC con *liquid clustering* por `(event_date, lat, lon)`. Para Q1 lee 149 veces menos bytes que el CSV y 4,6 veces menos que la Delta particionada por fecha; para Q2, 95 y 3,1 veces menos. En el reporte de día completo lee lo mismo que la partición por fecha.
- Las cifras de V4 cambian un poco entre corridas (en tres ejecuciones, Q1 y Q2 leyeron 1 o 2 archivos de 40 a 90 MB), porque el *clustering on write* no siempre corta los archivos igual. La conclusión no cambia.
- **`OPTIMIZE`**: con cargas diarias en serverless no reescribió nada (los archivos ya estaban en el tamaño objetivo y V4 ya venía agrupada desde la escritura). Con ingesta en micro-lotes (V5), la compactación pasó de 448 a 19 archivos y redujo los bytes un 24 %, **pero no mejoró la poda por zona** (Q1 leyó 190 MB). El `ZORDER` posterior sí la dio: 1 archivo de 50 MB, al nivel de V4. Lo que reduce la lectura es el orden de los datos por las columnas del filtro, no el número de archivos.
- La tabla final es `analytics.ais_positions_serving` (19 archivos, 1,2 GB, `OPTIMIZE FULL`).

### 4.5 Gobernanza (requisito 5)
- Catálogo `oceanwatch` con 4 esquemas por propósito y 2 Volumes.
- Comentarios en catálogo, esquemas, volúmenes, tablas y columnas (unidades, dominio y centinelas AIS).
- `TBLPROPERTIES` de linaje (`oceanwatch.source`, `produced_by`, `period`, `quality_status`) y etiquetas de dominio, capa, fuente y sensibilidad.
- Evidencia en `information_schema`.

## 5. Decisiones técnicas y su evidencia
Resumen en `BITACORA.md` (registro de decisiones). Cada decisión está justificada en su notebook con planes de ejecución, bytes o archivos leídos.

## 6. Limitaciones conocidas
- En serverless no hay `cache()`: los resultados intermedios reutilizados se **materializan** como tablas Delta pequeñas (`materialize()` en 03).
- Los tiempos en serverless son indicativos (hay caché de disco y variabilidad de recursos). La evidencia de rendimiento se basa en bytes y archivos leídos.
- "Archivos candidatos" en Delta se calcula reproduciendo el *data skipping* con el min/max real de cada archivo vía `_metadata`. Coincide con la regla del motor, pero no es la métrica interna del *query profile*.
- Las regiones marítimas son cajas lat/lon aproximadas. El análisis fino usa H3.

## 7. Estructura del repositorio
```
README.md          este documento
BITACORA.md        aporte por clase + registro de decisiones
notebooks/         00_config.py … 05_gobernanza.py (formato fuente de Databricks)
exports/           notebooks exportados con resultados (HTML / .ipynb) desde Databricks
```
