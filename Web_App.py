import polars as pl  
import streamlit as st
import altair as alt



# Importiamo i dati
Movies =  pl.read_csv("16k_Movies.csv") #importo il file csv


#################
# Puliamo il Dataset
#################

########################
# Creiamo una funzione per convertire il tempo in minuti per facilità
########################

# Funzione per convertire il tempo in Minuti
def tempo_in_minuti(testo):
    # Caso in il valore è null
    if not testo:
        return "null"
    ore = 0
    minuti = 0
    # Separiamo le ore dai MInuti
    if "h" in testo:
        parti = testo.split("h")
        try:
            # Elimino gli spazi
            ore = int(parti[0].strip())
        except ValueError:
            return "null"
    if "m" in testo:
        # Ottengo la stringa dei minuti
        parti = testo.split("m")[0].strip()
        if "h" in parti:
            parti = parti.split("h")[1].strip()
        try:
            minuti = int(parti)
        except ValueError:
            if ore == 0:
                return "null"
    # Ottengo il Valore finale sommando Ore e Minuti
    if ore > 0 or minuti > 0:
        return ore * 60 + minuti
    else:
        return "null"  

# Funzione per creare la Lista
def generazione_lista_minuti(lista):
    n = len(lista)
    new_lista = [0] * n
    for i in range(0, n):
        new_lista[i] = tempo_in_minuti(lista[i])
    return new_lista
# Creo la colonna
minuti = generazione_lista_minuti(Movies.select(pl.col("Duration")).to_series().to_list())

# Inserisco la colonna nel dataset
Movies = Movies.with_columns(
    pl.Series("Minutes", minuti, strict=False)
)
# print(Movies)


###################
# Pulisco la Data
###################

date = Movies.select(pl.col("Release Date")).to_series().to_list()

# Costruisco una funzione per scomporre la data
def ottieni_data(data):

        l = data.split(",")
        anno = int(l[1].strip())
        giorno = int(l[0].split(" ")[1].strip())
        mese = str(l[0].split(" ")[0])
        return (giorno, mese,anno)

# Costruisco una funzione per allocare i valori, data una lista in input
def generazione_lista_data(lista):
    n = len(lista)
    anno = [0] * n
    mese = [0] * n
    giorno = [0] * n
    for i in range(0, n):
        anno[i] = ottieni_data(lista[i])[2]
        mese[i] = ottieni_data(lista[i])[1]
        giorno[i] = ottieni_data(lista[i])[0]
    return((anno, mese, giorno))


# Ottengo il giorno
giorno = generazione_lista_data(date)[2]

Movies = Movies.with_columns(
    pl.Series("Day", giorno, strict=False)
)


# Ottengo il mese
mese = generazione_lista_data(date)[1]

Movies = Movies.with_columns(
    pl.Series("Month", mese, strict=False)
)


# Ottengo l'anno
anno = generazione_lista_data(date)[0]

Movies = Movies.with_columns(
    pl.Series("Year", anno, strict=False)
)

###################
# Pulisco i Generi 
###################

# Otteniamo la lista dei generi unici

generi = Movies.select(pl.col("Genres")).to_series().to_list()

# Ottengo un Set di generi

generi_unici = {
    genere.strip()
    for item in generi if item is not None  # Filtra i None
    for genere in item.split(",") 
}
#print(generi_unici)
#{'Romance', 'Sci-Fi', 'Adventure', 'News', 'Drama', 'Animation', 'Fantasy', 
# 'Mystery', 'Western', 'Reality-TV', 'Unknown', 'Family', 'Crime', 'Game-Show', 
# 'Thriller', 'History', 'Documentary', 'Action', 'Sport', 'Biography', 'Horror', 
# 'Musical', 'Comedy', 'Music', 'Talk-Show', 'War', 'Film-Noir'}


def assegnazione_generi(df):
    # Separa i generi in liste, gestendo i valori nulli o vuoti
    df = df.with_columns(
        pl.col(df.columns[9])
        .str.split(",").alias("GenreS")
    )
    
    #  Clono le righe per ogni genere
    df = df.explode("GenreS")
    
    # SostituiscO eventuali valori nulli con "null"
    df = df.with_columns(
        pl.col("GenreS").fill_null("null")
    )
    
    return df

Moviest = assegnazione_generi(Movies)

########################
# Pulisco gli scrittori e Registi
########################
# Replico la stessa operazione fatta per i generi

def assegnazione_scrittori(df):
    # Separo gli scrittori in liste
    df = df.with_columns(
        pl.col(df.columns[7])
        .str.split(",").alias("Writer")
    )

    df = df.explode("Writer")

    df = df.with_columns(
        pl.col("Writer").fill_null("null")
    )
    
    return df
Moviest = assegnazione_scrittori(Moviest)

def assegnazione_registi(df):

    df = df.with_columns(
        pl.col(df.columns[6])
        .str.split(",").alias("Director")
    )

    df = df.explode("Director")

    df = df.with_columns(
        pl.col("Director").fill_null("null")
    )
    
    return df
Moviest = assegnazione_registi(Moviest)

# Aggiungo l'anno al nome del film per identificarli meglio 
Moviest= Moviest.with_columns(
    (pl.col("Title") + " (" + pl.col("Year").cast(pl.Utf8) + ")").alias("Title")
)
Moviest = Moviest.drop(["Written by", "Duration"])
Moviest = Moviest.with_columns(Moviest["Director"].str.strip_chars())

###########
# Pulisco il numero di voti

# Rimuovo la virgola nella stringa per evitare mal'interpretazioni
Moviest = Moviest.with_columns(
    pl.col("No of Persons Voted").str.replace_all(",", "").alias("No of Persons Voted Senza Virgola")
)




##############################################################################

#########################################
# Analisi Esplorativa Iniziale          #
#########################################
# Imposto il layout della pagina
st.set_page_config(layout="wide")
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.markdown("<h1 style='font-size: 45px;'>Analisi Dei Film</h1", unsafe_allow_html=True)
    st.markdown("""In questa analisi esamineremo un insieme di film considerando cinque aspetti principali:
             il genere, il rating assegnato, gli autori, la durata e l’anno di uscita.        
            L'obiettivo è individuare tendenze e caratteristiche ricorrenti,
             osservando come questi elementi influenzino il successo e la percezione dei film.
            Analizzeremo i generi più rappresentati, i punteggi ricevuti, il ruolo di registi e sceneggiatori,
            infine la durata e la sua possibile correlazione con il gradimento.
            Inoltre, prenderemo in considerazione l’anno di uscita per capire come
            le tendenze cinematografiche siano cambiate nel tempo.
            Questa analisi ci aiuterà a tracciare un quadro generale
            sui film, sulle loro principali caratteristiche e le loro relazioni. I dati sono stati presi da Kaggle
            [Vai al Sito](https://www.kaggle.com/datasets/kashifsahil/16000-movies-1910-2024-metacritic), e derivano a loro volta dal sito Metacritict
            [Vai al Sito](https://www.metacritic.com/). Abbiamo a disposizione un dataset con all'incirca 15000 film, 
            questo è solo un campione dei film totali e non abbiamo gli strumenti per capire se rappresentativo o meno.
            Quindi le conclusioni a cui giungeremo in questo studio verrano prese sotto l'assunzione di rappresentatività del campione 
""")


# Prima introduzione sui dati

    st.markdown("<h1 style='font-size: 40px;'>1. Visualizziamo il Campione</h1", unsafe_allow_html=True)
    st.markdown("""Prima di valutare le relazioni tra le varie variabili è necessario capire le caratteristiche del campione. Stiamo considerando un vasto numero di generi come:
            Romance, Sci-Fi, Adventure, Mystery, Drama, Animation, Fantasy... L'intervallo temporale preso in considerazione va dal
            1970 a circa metà 2024, i film presi in considerazione sono di durata variabile. Infine è necessario tenere in considerazione
            quante persone hanno valutato un film. \\
            Scopriamo meglio il nostro campione:

    """)

##################################################################à
######################## BARPLOT GENERI ###########################
###################################################################
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.markdown("<h1 style='font-size: 30px;'>1.1 Generi Cinematografici </h1", unsafe_allow_html=True)

# selezionamo le coppie titolo genere
Film_Generi_Unici= Moviest.select(pl.col('Title', 'GenreS')).unique()

# Calcoliamo il conteggio dei film per genere
Film_Generi_Count = Film_Generi_Unici.group_by('GenreS').agg(pl.count().alias('count'))

# Assegnamo i colori
genre_colors = {
    "Action": '#E63946',         # Rosso Vibrante
    "Adventure": '#F9844A',      # Arancione
    "Animation": '#F9C74F',      # Giallo Brillante
    "Biography": '#90BE6D',      # Verde
    "Comedy": '#00FF00',         # Verde Lime
    "Crime": '#4A4E69',          # Blu-Grigio Scuro
    "Documentary": '#577590',    # Blu Medio
    "Drama": '#0000FF',          # Blu
    "Family": '#80FFDB',         # Menta
    "Fantasy": '#9B5DE5',        # Viola chiaro
    "Film-Noir": '#14213D',      # Navy scuro
    "Game-Show": '#FF9F1C',      # Arancione-Oro
    "History": '#8A5A44',        # Marrone
    "Horror": '#540B0E',         # Rosso Scuro
    "Music": '#43AA8B',          # Verde The
    "Musical": '#F15BB5',        # Rosa caldo
    "Mystery": '#4E5283',        # Blu-Viola Scuro
    "News": '#4CC9F0',           # Blu chiaro
    "Reality-TV": '#F15025',     # arancione-rosso brillante
    "Romance": '#FF00FF',        # Magenta
    "Sci-Fi": '#2EC4B6',         # Turchese
    "Sport": '#2D7DD2',          # Blu Medio
    "Talk-Show": '#FFC6FF',      # Rosa Chiaro
    "Thriller": '#FF0000',       # Rosso Vivace
    "Unknown": '#BCBCBC',        # Grigio
    "War": '#606C38',            # Verde oliva
    "Western": '#DDA15E'         # Sabbia
}


# Grafico 
# Per ottenere l'effetto evidenziamento
highlight = alt.selection_single(
    on='mouseover',  # Attiva l'effetto al passaggio del mouse
    fields=['GenreS'],  # Variabile su cui applicare l'effetto
    empty='none'  # Nessun effetto quando non c'è selezione
)

# Creazione del bar chart con bordo condizionale
bar_chart = alt.Chart(Film_Generi_Count).mark_bar(
    cornerRadiusTopRight=5,
    cornerRadiusTopLeft=5,
    size=20,
    strokeWidth=2  # Spessore del bordo
).encode(
    x=alt.X('count:Q', title='Numero di Film'),
    y=alt.Y('GenreS:N', title='Genere', sort='-x'),
    color=alt.Color('GenreS:N').scale(
        domain=list(genre_colors.keys()),
        range=list(genre_colors.values())
    ).legend(None),
    stroke=alt.condition(  # Condizione per il bordo
        highlight,
        alt.value('black'),  # Bordo nero quando evidenziato
        alt.value(None)  # Nessun bordo altrimenti
    ),
    opacity=alt.condition( # inseriamo anche l'opacità
        highlight,
        alt.value(1), 
        alt.value(0.8) 
    )
)

# Aggiungo la selezione al grafico
bar_chart = bar_chart.add_selection(highlight)

# Aggiungo etichette 
labels = alt.Chart(Film_Generi_Count).mark_text(
    align='left',
    baseline='middle',
    dx=3,
    fontSize=12,
    color='black'
).encode(
    x=alt.X('count:Q'),
    y=alt.Y('GenreS:N', sort='-x'),
    text=alt.Text('count:Q')
)

# Combiniamo grafico e etichette
combined_chart = (bar_chart + labels).properties(
    width=600,
    height=700,
    padding = {"right": 20}
).configure_axis(
    labelFontSize=12,
    titleFontSize=14
)

# Mostra il grafico in Streamlit
col1, col2, col3, col4 = st.columns([1, 3, 5, 1])


with col2:
    st.write("### Distribuzione dei Generi")
    st.write("""
Il grafico a barre rappresenta la distribuzione dei film per genere,
basandosi su dati provenienti da una serie temporsle quindi possiamo vedere quali generi sono stati più prodotti da gli anni '70. 
La categoria più presente è il Drama, con 8.829 film, seguita da Comedy (4.862),
Thriller (3.623) e Romance (3.008). Questi generi dominano il panorama produttivo,
suggerendo una tendenza storica dell’industria cinematografica a concentrarsi su narrazioni emotive, 
coinvolgenti o di largo intrattenimento. Anche generi come Crime, Action, Documentary e Sci-Fi mantengono un ruolo rilevante,
pur con numeri inferiori.
L’andamento riflette non solo le preferenze del pubblico, ma anche le dinamiche culturali, sociali e tecnologiche che si sono 
evolute nel tempo. Ad esempio, l’affermazione dei documentari o della fantascienza può essere collegata a fasi storiche specifiche
e all’accesso a nuove tecnologie o piattaforme. I generi con minore frequenza, come Musical, Western, Film-Noir o Talk-Show, sembrano
invece appartenere a nicchie o a periodi passati, oggi molto meno rappresentati. Inoltre, la presenza di categorie come null o Unknown
suggerisce che alcuni dati non siano stati correttamente etichettati, probabilmente a causa di variazioni negli standard di classificazione
nel tempo. Nel complesso, il grafico offre una panoramica delle tendenze storiche nella produzione cinematografica per genere, 
mostrando chiaramente quali categorie si siano affermate o ridimensionate nel corso degli anni.
    """)

# Grafico nella colonna a destra
with col3:
    st.altair_chart(combined_chart, use_container_width=True)

#########################################################################
####################    Serie Anni         ##############################
#########################################################################
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.markdown("<h1 style='font-size: 30px;'>1.2 Film per Anno e Genere</h1>", unsafe_allow_html=True)

# creo le cache per memorizzare le informazioni ed evitare che vengano selezionati in modo errato i generi
@st.cache_data 
def load_data():
    return Moviest.select(pl.col('Title', 'Year', 'GenreS')).unique()
Film_Anni_Generi = load_data()

# Raggruppo i generi per anno
Film_Anni_Generi = (
    Film_Anni_Generi
    .group_by(pl.col("Year"), pl.col("GenreS"))
    .agg(Numero_Di_Film=pl.col("Title").count())
)


# Ricavo il totale
@st.cache_data
def somma_film_anno():
    Somma_anni = Moviest.select(pl.col('Title', 'Year')).unique()
    Somma_anni = (Somma_anni.group_by(pl.col('Year'))
        .agg(Numero_Di_Film=pl.col("Title").count())    
    )
    return(Somma_anni)
Yearsum = somma_film_anno()
Yearsum = Yearsum.with_columns(pl.lit("Total").alias("GenreS"))

# Unisci i due dataset
Yearsum = Yearsum.select(pl.col("Year", "GenreS", "Numero_Di_Film"))
Film_Anni_Generi = pl.concat([Film_Anni_Generi, Yearsum])

genre_colors["Total"] = "black" 

# Ottieni tutti gli anni e generi univocamente in 2 dataset
all_years = Film_Anni_Generi.select(pl.col("Year")).unique().sort("Year") # ordino per gli anni
all_genres = Film_Anni_Generi.select(pl.col("GenreS")).unique()

# Crea un dataset completo con tutte le combinazioni di Year e GenreS
complete_data = all_years.join(all_genres, how="cross")


# Creo il dataset
Film_Anni_Generi_completo = complete_data.join(
    Film_Anni_Generi, on=["Year", "GenreS"], how="left"
).fill_null(0)



# Estrai tutti i generi unici
@st.cache_data 
def load_data2():
    return Film_Anni_Generi.select(pl.col("GenreS")).unique()["GenreS"]


unique_genres = load_data2()

# Creo uno spazio per memorizzare i generi
if "selected_genres" not in st.session_state:
    st.session_state.selected_genres = ["Total", "Drama", "Comedy", "Thriller"]


col1, col2, col3 = st.columns([1, 8, 1])
# box da selezionare
with col2:
    selected_genres = st.multiselect(
    'Seleziona i generi da visualizzare', 
    unique_genres, 
    default=st.session_state.selected_genres
    )

# Aggiorna se è stata fatta una modifica
if selected_genres != st.session_state.selected_genres:
    st.session_state.selected_genres = selected_genres

# Filtra i dati in base ai generi selezionati
filtered_data = Film_Anni_Generi_completo.filter(pl.col("GenreS").is_in(st.session_state.selected_genres))

# Creo un il grafico
chart = (
    alt.Chart(filtered_data)
    .mark_line()
    .encode(
        x=alt.X('Year:O'),
        y=alt.Y('Numero_Di_Film:Q', scale=alt.Scale(domain=[0, 700])),
        color=alt.Color('GenreS:N').scale(
            domain=list(genre_colors.keys()),
            range=list(genre_colors.values())
        ).legend(None)
    )
)

# Disegno le linee
line = alt.Chart(filtered_data).mark_line().encode(
    x=alt.X("Year:O"),
    y=alt.Y("Numero_Di_Film:Q", scale=alt.Scale(domain=[0, 700])),
    color=alt.Color("GenreS:N").scale(
        domain=list(genre_colors.keys()),
        range=list(genre_colors.values())
    ).legend(None)
)

# Trova il massimo valore di Year 
last_values = filtered_data.group_by("GenreS").agg(pl.col("Year").max().alias("LastYear"))

# Filtro il dataset per mantenere solo i valori dell'ultimo anno per ogni genere
last_points = filtered_data.join(last_values, left_on=["GenreS", "Year"], right_on=["GenreS", "LastYear"])

# Disegna i pallini alle estremità
circle = (
    alt.Chart(last_points)
    .mark_circle(size=60)
    .encode(
        x="Year:O",
        y=alt.Y("Numero_Di_Film:Q", scale=alt.Scale(domain=[0, 700])),
        color=alt.Color("GenreS:N").scale(
            domain=list(genre_colors.keys()),
            range=list(genre_colors.values())
        ).legend(None)
    )
)

# Aggiungo le etichette
text = (
    alt.Chart(last_points)
    .mark_text(align="left", dx=4, fontSize=12)
    .encode(
        x="Year:O",
        y=alt.Y("Numero_Di_Film:Q", scale=alt.Scale(domain=[0, 700])),
        text="GenreS",
        color=alt.Color("GenreS:N").scale(
            domain=list(genre_colors.keys()),
            range=list(genre_colors.values())
        ).legend(None)
    )
)


# Combina gli elementi
final_chart = line + circle + text


col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.altair_chart(final_chart, use_container_width=True)
    st.info("**Nota: molti film hanno piu' generi associati, quindi sommando le varie quantità per ogni genere non si ottiene il totale**")
    st.markdown(""" Da questo grafico possiamo ricavare: per prima cosa che è avvenuto un generale incremento da gli 
                anni 2000 in poi, in particolare possiamo notare un picco nel 2014 in cui sono state registrate circa 650 osservazioni.
                Alcuni generi hanno poche o nessuna registrazione in determinati periodi. 
                Quando analizzaremo grafici in futuro non possiamo ignorare queste informazioni
             """)


######################################################################
##################       Barplot durata   ############################
######################################################################
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.markdown("<h1 style='font-size: 30px;'>1.3 Durata </h1", unsafe_allow_html=True)

# Filtriamo i Valori unici

Film_Minuti_Unici = Moviest.select(pl.col('Title', 'Minutes')).unique()

# Filtriamo le rig he con "Minutes" == "null"
Riga_null = Film_Minuti_Unici.filter(pl.col("Minutes") == "null")
# Aggiungiamo il Conto
Riga_null = Riga_null.group_by("Minutes").agg(Numero_Di_Film=pl.col("Title").count())

# Filtriamo solo i valori numerici in "Minutes"
Film_Minuti_Count = Film_Minuti_Unici.filter(pl.col("Minutes") != "null")

# Convertiamo i valori numerici
Film_Minuti_Count = Film_Minuti_Count.with_columns(
    pl.col("Minutes").str.strip_chars().cast(pl.Int64, strict=False)  # Rimuove spazi e converte
)

# Raggruppiamo per "Minutes" e contiamo i film
Film_Minuti_Count = (
    Film_Minuti_Count.group_by("Minutes")
    .agg(Numero_Di_Film=pl.col("Title").count())
    .sort("Minutes")
)




# Feature del Mouse
highlight = alt.selection_single(
    on='mouseover', 
    fields=['Minutes'], 
    empty='none' 
)

# Creazione del bar chart con bordo condizionale
bar_chart = alt.Chart(Film_Minuti_Count).mark_bar(
    cornerRadiusTopRight=5,
    cornerRadiusTopLeft=5,
    size=3.5,
    strokeWidth= 0.8  # Spessore del bordo
).encode(
    x=alt.X('Minutes:N', title='Minutaggio', sort='ascending' ),
    y=alt.Y('Numero_Di_Film:Q', title='Numero Di Film'),
    stroke=alt.condition(  # Condizione per il bordo
        highlight,
        alt.value('black'),  # Bordo nero quando evidenziato
        alt.value(None)  # Nessun bordo altrimenti
    ),
    color=alt.condition(  # Condizione per il colore
        highlight,
        alt.value('yellow'),  # Colore giallo quando il mouse è sopra
        alt.value('steelblue') 
    ),
    opacity=alt.condition(  # Opacità 
        highlight,
        alt.value(1),  
        alt.value(0.8)
    )
).properties(
    title="Distribuzione del Numero di Film per Minutaggio",
    width=800,  
    height=400 
)

# Aggiunge la selezione al grafico
bar_chart = bar_chart.add_selection(highlight)

col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.altair_chart(bar_chart)
    st.markdown(""" La distribuzione di questo grafico sembra avere una mediana che si può stimare
                 graficamente attorno al valore 100. Notiamo anche
                alcuni valori straordinari come 90, 100, 105 etc. una risposta la si può intuire in 
                un arrotondamento nel caricamento sul sito del film, in quanto preferiamo valori
                 a "cifre tonde",
                oppure perchè possono essere stati imposti vincoli sulla durata per la produzione del film.
                Infine notiamo il valore 90 che spicca su tutti possiamo pensare che possa esser stato uno
                standard nella produzione di film.    
             """)

######################################################################
################## Barplot Voti ############################
######################################################################
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.markdown("<h1 style='font-size: 30px;'>1.4 Numero di Valutazioni</h1>", unsafe_allow_html=True)

# Filtriamo e prendo i film unici
Film_Voti_Unici = Moviest.select('Title', 'No of Persons Voted').unique()

# Filtriamo eliminando i valori "null" 
Film_Voti_Count = Film_Voti_Unici.filter(
 (pl.col("No of Persons Voted") != "null")  
)

# Convertiamo la colonna in numerico
Film_Voti_Count = Film_Voti_Count.with_columns(
    pl.col("No of Persons Voted").str.strip_chars().cast(pl.Int64, strict=False).alias("Votes")
)

# Divido in Classi
etichette = ["1-9", "10-19", "20-49", "50-99", "100-199", "200+"]

Film_Voti_Count = Film_Voti_Count.with_columns(
    pl.when(pl.col("Votes") < 10)
      .then(pl.lit(etichette[0]))
    .when(pl.col("Votes") < 20)
      .then(pl.lit(etichette[1]))
    .when(pl.col("Votes") < 50)
      .then(pl.lit(etichette[2]))
    .when(pl.col("Votes") < 100)
      .then(pl.lit(etichette[3]))
    .when(pl.col("Votes") < 200)
      .then(pl.lit(etichette[4]))
    .otherwise(pl.lit(etichette[5]))
    .alias("Vote Class")
)


Film_Voti_Count = Film_Voti_Count.group_by("Vote Class").agg(Numero_Di_Film=pl.col("Vote Class").count())


# Imposto l'evidenziazione
highlight = alt.selection_single(
    on='mouseover',
    fields=['Vote Class'],
    empty='none'
)

# Costruisco il grafico
bar_chart = alt.Chart(Film_Voti_Count).mark_bar(
    cornerRadiusTopRight=5,
    cornerRadiusTopLeft=5,
    size=100,
    strokeWidth=1.5
).encode(
    x=alt.X('Vote Class', 
            title='N. Valutazioni',
              sort=["1-9", "10-19", "20-49", "50-99", "100-199", "200+"]),
    y=alt.Y('Numero_Di_Film:Q', title='Numero Di Film'),
    stroke=alt.condition(highlight, alt.value('black'), alt.value(None)),
    color=alt.condition(highlight, alt.value('yellow'), alt.value('steelblue')),
    opacity=alt.condition(highlight, alt.value(1), alt.value(0.8)),
    tooltip=['Vote Class', 'Numero_Di_Film']
).properties(
    title="Distribuzione del Numero di Valutazioni per Film",
    width=800,
    height=400
)

# Aggiungiamo la selezione al grafico
bar_chart = bar_chart.add_selection(highlight)


col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.altair_chart(bar_chart)
    st.markdown("""
    È importante valutare anche quante persone hanno valutato un film, se 
    è maggiormente votato possiamo pensare che abbia una valutazione più
    precisa. È importante tenere in considerazione durante le analisi che una buona fetta 
    di film ha ricevuto meno di 10 valutazioni.
                """)


######################################################################
################## Barplot Rating ############################
######################################################################

col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.markdown("<h1 style='font-size: 30px;'>1.5 Rating </h1>", unsafe_allow_html=True)  

# Filtro e prendo le coppie 
Film_Rating_Unici = Moviest.select(pl.col('Title', 'Rating')).unique()  

# Elimino i valori null
Film_Rating_Count = Film_Rating_Unici.filter(pl.col("Rating").is_not_null())  

# Ordino i rating
Film_Rating_Count = Film_Rating_Count.group_by("Rating").agg(
    pl.col("Rating").count().alias("Numero_Di_Film")
).sort("Rating")

# Converto in numerico

Film_Rating_Count = Film_Rating_Count.with_columns(
    pl.col("Rating").cast(pl.Utf8)  
)

# Elimino i valori null

Film_Rating_Count = Film_Rating_Count.filter(pl.col("Rating").is_not_null())



# Creiamo la selezione evidenziata
highlight = alt.selection_single(
    on='mouseover',  
    fields=['Rating'],  
    empty='none'  
)

# Creiamo il grafico a barre
bar_chart2 = alt.Chart(Film_Rating_Count).mark_bar(  
    cornerRadiusTopRight=5,
    cornerRadiusTopLeft=5,
    size=7,
    strokeWidth=2  
).encode(
    x=alt.X('Rating:N', title='Rating', sort='ascending'),
    y=alt.Y('Numero_Di_Film:Q', title='Numero Di Film'),
    stroke=alt.condition(
        highlight,
        alt.value('black'),  
        alt.value(None)  
    ),
    color=alt.condition(
        highlight,
        alt.value('yellow'),  
        alt.value('steelblue')  
    ),
    opacity=alt.condition(
        highlight,
        alt.value(1),  
        alt.value(0.8)  
    )
).properties(
    title="Distribuzione del Numero di Film per Rating",
    width=800,  
    height=400
).add_selection(highlight)  

# Visualizziamo il grafico con Streamlit
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.altair_chart(bar_chart2)
    st.markdown(""" Gran parte delle valutazioni medie sta tra 6 e 8, con un picco per il valore 7.
                Tuttavia esistono film con valutazioni medie molto basse (anche sotto 3) e molto alte (maggiori di 9.5). """)


############################################################################àà
##############################################################################
###############################################################################
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.markdown("<h1 style='font-size: 40px;'>2 Analisi dei Dati</h1", unsafe_allow_html=True)
    st.markdown("""L’analisi dei dati cinematografici si concentrerà su diversi aspetti chiave per comprendere l’evoluzione del cinema nel tempo. In particolare, verranno esaminate le variazioni nei generi cinematografici, l’impatto dei registi più influenti, la relazione tra durata dei film e rating, nonché le tendenze generali dell’apprezzamento del pubblico.
    Per questo studio, utilizzeremo informazioni dettagliate su ciascun film, inclusi il genere, il regista, la durata e il rating. Analizzeremo come questi fattori si siano modificati nel tempo, cercando di individuare pattern significativi e possibili correlazioni. Inoltre, studieremo l’eventuale affermazione di nuovi generi o la decadenza di altri, osservando come le preferenze del pubblico siano cambiate nel corso degli anni.
    L’obiettivo è fornire un quadro quantitativo e qualitativo dell’evoluzione del cinema, sfruttando i dati per supportare ipotesi e trarre conclusioni significative.
    """)
 
#################################################################################à
################# boxplot media voti generi######################################
##################################################################################

Film_Anni_Generi = Moviest.unique(subset=['Title', 'Year', "GenreS"])

# Rimuovo i film con valori nulli per l'anno e "null" o nulli per il genere
Film_Anni_Generi2 = (
    Film_Anni_Generi.filter(
        (pl.col("Year").is_not_null()) & 
        (pl.col("GenreS").is_not_null()) & 
        (pl.col("GenreS") != "null") 
    )
)
#{'Romance', 'Sci-Fi', 'Adventure', 'News', 'Drama', 'Animation', 'Fantasy', 
# 'Mystery', 'Western', 'Reality-TV', 'Unknown', 'Family', 'Crime', 'Game-Show', 
# 'Thriller', 'History', 'Documentary', 'Action', 'Sport', 'Biography', 'Horror', 
# 'Musical', 'Comedy', 'Music', 'Talk-Show', 'War', 'Film-Noir'}

# Elimino i generi con troppe poche osservazioni

Film_Anni_Generi2 = Film_Anni_Generi2.filter(~pl.col("GenreS").is_in(['Game-Show', 'Reality-TV', 'Talk-Show', 'Film-Noir', 'null', 'Unknown']))

# Ordino i generi in base alla mediana
order = (
    Film_Anni_Generi2.group_by("GenreS")
    .agg(pl.median("Rating").alias("Median")) 
    .sort(["Median", "GenreS"], descending = True) 
    .select("GenreS") 
    .to_series() 
    .to_list() 
) 

# creo il boxplot

Rating_boxplot = (
    alt.Chart(Film_Anni_Generi2)  
    .mark_boxplot()
    .encode(
        x=alt.X("GenreS:N", title="Genere Di Film", sort=order), 
        y=alt.Y("Rating:Q", title="Valutazione Media Dei Film"), 
        color=alt.Color('GenreS:N').scale(
            domain=list(genre_colors.keys()),
            range=list(genre_colors.values())
        ).legend(None)
    )    
    .properties(
        title="Distribuzione del Rating per ogni Genere",  
        height=500
    )
    .configure_title(fontSize=20)  
)
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.markdown("<h1 style='font-size: 30px;'>2.1 Visualizzazione del rating rispetto a i Generi</h1", unsafe_allow_html=True)
    st.altair_chart(Rating_boxplot, use_container_width=True)
    st.markdown("""
    Il genere con mediana maggiore è News seguito, da i film di guerra, sulla musica, storia,
    documentari, animazione etc., tuttavia la differenza di rating è minima passando da una mediana di 7.25 ad una di 7.
    I generi invece con valutazioni mediane più basse sono Sci-fi, mistery, horror, i quali
    si basano spesso su elementi fantastici o mistici che non vengo sempre apprezzati come da aspettative.
""")

######################################################
#    Evoluzione del rating nel tempo
######################################################
col1, col2, col3 = st.columns([1, 8, 1])


# Assegno ogni film ad ogni suo anno e genere e calcolo la media
Film_Anni_Generi3 = (
    Film_Anni_Generi
    .unique(subset=["Title", "Year", "GenreS"])  
    .group_by(["GenreS", "Year"])
    .agg(
        Numero_Di_Film = pl.col("Title").count(),  
        Media_Voto = pl.col("Rating").mean()
    )
)

# Rimuovo i valori null
Film_Anni_Generi3 = Film_Anni_Generi3.filter(pl.col("Media_Voto").is_not_null())

#  Creo la colonna di "Total"
Yearsum = (
    Film_Anni_Generi
    .unique(subset=["Title", "Year"])  # ← ignora i generi
    .group_by("Year")
    .agg(
        Numero_Di_Film = pl.count("Title"),
        Media_Voto = pl.mean("Rating")
    )
    .with_columns(pl.lit("Total").alias("GenreS"))
)

Yearsum = Yearsum.select(pl.col("GenreS", "Year", "Numero_Di_Film", "Media_Voto"))

# Aggiungo le righe di Total al dataframe esistente
Film_Anni_Generi3 = pl.concat([Film_Anni_Generi3, Yearsum])

# complete data definito in precedenza (circa riga 420 commit finale)
# riempio le combinazioni

Film_Anni_Generi_completo2 = complete_data.join(
    Film_Anni_Generi3, on=["Year", "GenreS"], how="left"
).fill_null(0)

# creo delle cache per salare le modifiche interattive
@st.cache_data 
def load_data10():
    return Film_Anni_Generi_completo2.select(pl.col("GenreS")).unique()["GenreS"]
unique_genres = load_data10()

# creo uno spazio per salvare i generi selezionati
if "selected_genres2" not in st.session_state:
    st.session_state.selected_genres2 = ["Total", "Drama", "Comedy", "Thriller"]

# seleziono i generi da vedere
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    selected_genres2 = st.multiselect(
    'Seleziona i generi da visualizzare', 
    unique_genres, 
    default=st.session_state.selected_genres2
    )

# Aggiorno le modifiche
if selected_genres2 != st.session_state.selected_genres2:
    st.session_state.selected_genres2 = selected_genres2

# Filtra i dati in base ai generi selezionati
filtered_data = Film_Anni_Generi_completo2.filter(pl.col("GenreS").is_in(st.session_state.selected_genres2))

# Creo un chart base
chart = (
    alt.Chart(filtered_data)
    .encode(
        color=alt.Color('GenreS:N').scale(
            domain=list(genre_colors.keys()),
            range=list(genre_colors.values())
        ).legend(None)
    )
)

# Disegno la linea
line = chart.mark_line().encode(
    x=alt.X("Year:O"),
    y=alt.Y("Media_Voto:Q")
)

# Trovo il massimo valore di Year per ogni genere
last_values = filtered_data.group_by("GenreS").agg(pl.col("Year").max().alias("LastYear"))

# Filtro il dataset per mantenere solo i valori dell'ultimo anno disponibile per ogni genere
last_points = filtered_data.join(last_values, left_on=["GenreS", "Year"], right_on=["GenreS", "LastYear"])

# Disegno i pallini alle estremità
circle = (
    alt.Chart(last_points)
    .mark_circle(size=60)
    .encode(
        x="Year:O",
        y="Media_Voto:Q",
        color="GenreS:N"
    )
)

# Aggiungo le etichette
text = (
    alt.Chart(last_points)
    .mark_text(align="left", dx=4, fontSize=12)
    .encode(
        x="Year:O",
        y="Media_Voto:Q",
        text="GenreS",
        color="GenreS:N"
    )
)

# Combino gli elementi
final_chart = line + circle + text

# Visualizzazione in Streamlit
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.markdown("<h1 style='font-size: 30px;'>2.2 Evoluzione del Rating medio nel Tempo </h1", unsafe_allow_html=True)
    st.altair_chart(final_chart, use_container_width=True)
    st.markdown("""Dal grafico possiamo notare che la qualità media dei film è calata. Questo può
                essere causato come abbiamo visto in precedenza da un aumento della produzione dei film.
                È possibile interagire col grafico e vedere l'andamento dei generi a scelta. Bisogna porre attenzione
                a comparare generi e trarre conclusioni affrettate, in quanto si consiglia anche di guardare la numerosità 
                per anno dei generi calcolata precedentemente.
    """)


##########################à##############################################################################
########################### Durata Rating ###############################################################
#########################################################################################################
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.markdown("<h1 style='font-size: 30px;'>2.3 Confronto tra Durata e Rating </h1", unsafe_allow_html=True)

# Rimuovo i duplicati mantenendo solo un'istanza per titolo 
Film_Ore_Rating = Moviest.unique(subset=['Title'])

# Filtro solo i film con valore di rating non nullo e durata diversa da "null"
Film_Ore_Rating = Film_Ore_Rating.filter(
    pl.col("Rating").is_not_null() & (pl.col("Minutes") != "null")
)

# Converto la durata da stringa a float
Film_Ore_Rating1 = Film_Ore_Rating.with_columns(pl.col("Minutes").cast(pl.Float64))

# Filtro i film con durata ragionevole (elimino outlier > 350 minuti)
Film_Ore_Rating = Film_Ore_Rating1.filter(pl.col("Minutes") <= 350)

# Converto il DataFrame Polars in Pandas, necessario per il grafico
film_df = Film_Ore_Rating.to_pandas()

# Creo uno slider in Streamlit per impostare una soglia in minuti
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    valore_soglia = st.slider("Soglia (Minuti)", min_value=0, max_value=350, value=5, step=1)

# Definisco dimensioni del grafico base
base = alt.Chart(film_df).properties(
    width=500,
    height=400
)

# Creo il livello del grafico a dispersione per i film con durata >= soglia
grafico_dispersione = base.mark_circle().encode(
    x=alt.X("Minutes:Q", scale=alt.Scale(domain=[0, 350])).title("Minuti"),
    y=alt.Y("Rating:Q").title("Rating")
).transform_filter(
    alt.datum.Minutes >= valore_soglia
)

# Raggruppo i film graficamente con durata < soglia
grafico_bin = base.mark_circle().encode(
    x=alt.X("Minutes:Q", scale=alt.Scale(domain=[0, 350])).bin(maxbins=10),
    y=alt.Y("Rating:Q").bin(maxbins=10),
    size=alt.Size("count():Q").scale(domain=[0, 2000])
).transform_filter(
    alt.datum.Minutes < valore_soglia
)

# Aggiungo una linea rossa verticale per evidenziare la soglia
linea = alt.Chart(pl.DataFrame({'x': [valore_soglia]})).mark_rule(color="red").encode(
    x='x:Q',
    size=alt.value(2)
).properties(
    height=400
)

# Combino i layer
grafico = alt.layer(grafico_dispersione, grafico_bin, linea)


col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.altair_chart(grafico, use_container_width=True)

correlation = Film_Ore_Rating1.select(pl.corr("Minutes", "Rating")).to_series()[0]


col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.write("Correlazione:", correlation)
    st.markdown(""" La correlazione tra la durata dei film e il rating è molto bassa.
                Questo significa che non è presente una relazione diretta forte tra 
                il minutaggio e la valutazione.
                """)



##########################à##############################################################################
########################### Durata Rating ###############################################################
#########################################################################################################
with col2:
    st.markdown("<h1 style='font-size: 30px;'>2.3 Confronto tra Durata e Anni</h1>", unsafe_allow_html=True)

# Rimuove i duplicati basandosi solo sul titolo
unicidr = Moviest.unique(subset=["Title"])
unicidr = unicidr.with_columns(
    pl.when(pl.col("Minutes") == "null")
      .then(None)
      .otherwise(pl.col("Minutes"))
      .alias("Minutes")
)

# Converto a Float64 e rimuove le righe con nulli
unicidr = unicidr.with_columns(
    pl.col("Minutes").cast(pl.Float64)
).drop_nulls(["Minutes", "Year"])

# Raggruppa per anno e calcolo durata media e conteggio
Dati_durat = (
    unicidr
    .group_by("Year")
    .agg([
        pl.col("Minutes").mean().alias("Media Minuti"),
        pl.count().alias("Numero Di Film")
    ])
)

# Creo il grafico
grafico_durat = alt.Chart(Dati_durat).mark_trail().encode(
    x='Year:Q',
    y='Media Minuti:Q',
    size=alt.Size('Numero Di Film:Q', scale=alt.Scale(range=[1, 10]))
).properties(
    width=1200,   
    height=400   
)

with col2:
    st.altair_chart(grafico_durat)
    st.markdown("""
        Il grafico mostra l'evoluzione della durata nel tempo, una linea più spessa
                indica una maggiore quantità di film. Questo grafico è stato fatto 
                per rispondere alla domanda: è vero che la durata dei film si è ridotta
                negli ultimi anni? La risposta è non proprio negli ultimi anni, si può pensare 
                che una riduzione della soglia dell'attenzione dovuta anche dei social
                abbia avuto un impatto sull'industria cinematografica, ma la media come possiamo vedere
                è calata già prima dalla metà degli anni '80 passando da 118 il picco assoluto
                a valori inferiori di 110 minuti che poi si sono stabilizzati tra i 100 ed i 110 minuti.
                Quindi almeno da questi dati e questo grafico possiamo vedere che è falsa questa convinzione.
                Traiamo queste conclusioni sotto l'assunzione di rappresentitatività del campione.
""")
##################################################################à
######################## Analisi FIlm Dettaglio ###################
###################################################################



col1, col2, col3 = st.columns([1, 8, 1])
with col2:

    st.markdown("<h1 style='font-size: 40px;'>3 Film nel dettaglio</h1", unsafe_allow_html=True)
    st.markdown("""In questa sezione è possibile direttamente  il dataset
                 in modo attivo, senza doversi
                 accontentare di analisi precedenti.
                 Si ha la possibilità di scoprire quali sono i film migliori
                 per ogni epoca, analizzando i periodi storici e generi a cui si è maggiormente interessati,
                 e di individuare i registi più rilevanti 
                sulla base della qualità media delle loro opere,
                della quantità di film prodotti o di altri criteri oggettivi.
                Inoltre, grazie a un algoritmo di ricerca, è possibile scoprire
                i film inserendo direttamente titolo, genere, anno 
                o altri parametri.
                

    """)

###################################################################
########### Grafico film per ogni decade ##########################
###################################################################
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.markdown("<h1 style='font-size: 30px;'>3.1 Film Migliori per ogni Genere e Decade</h1", unsafe_allow_html=True)
    st.markdown("""Qui è sotto è possibile scoprire quali sono i film migliori per 
                   ogni decade e genere, come abbiamo visto precedentemente molti film
                   hanno un numero ridotto di valutazioni, per evitare che film
                    con troppe poche valutazioni finiscano in questa classifica
                    è possibile selezionare anche il numero minimo di valutazioni che deve
                    avere un film per essere considerato.
                 """)
    
# aggiungo il genere "General" selezionando le terzine uniche
Film_agg = Moviest.unique(subset=["Title", "Rating", "Year"])
Film_agg = Film_agg.select(pl.col("Title", "Rating", "Year", 'No of Persons Voted'))
Film_agg = Film_agg.with_columns(pl.lit("General").alias("GenreS"))
Film_rank = Moviest.unique(subset=["Title", "Year", "GenreS"])
Film_rank = Film_rank.select(pl.col("Title", "Rating", "Year", 'No of Persons Voted', "GenreS"))
Film_rank = pl.concat([Film_rank, Film_agg])

# elimino i rating null
Film_rank = Film_rank.drop_nulls("Rating")

# definisco la funzione che classifica
def get_decade(year):
    if year < 1980:
        return '1970-1979'
    elif year < 1990:
        return '1980-1989'
    elif year < 2000:
        return '1990-1999'
    elif year < 2010:
        return '2000-2009'
    elif year < 2020:
        return '2010-2019'
    else:
        return '2020+'

# creo la funzione per ottenere la decade
Film_rank = Film_rank.with_columns(
    Decade = pl.col("Year").map_elements(get_decade)
)


# creo lo spazio per registrare il genere General di default
if "selected_genres3" not in st.session_state:
    st.session_state.selected_genres3 = "General"

# ottengo una colonna con i generi unici in una lista
@st.cache_data 
def load_data100():
    return Film_rank.select(pl.col("GenreS")).unique()["GenreS"]
unique_genres2 = load_data100()
unique_genres2_list = unique_genres2.to_list()

#rimuovo "null" e "Film-Noir(ha un osservazone senza anno)" dalle possibili scelte
unique_genres2_list = [g for g in unique_genres2.to_list() if g not in ["null", "Film-Noir"]]

# Implemento selectbox per i generi
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    selected_genres3 = st.selectbox(
    "Seleziona un genere:",
    options=unique_genres2_list
    )
# aggiungo anche funzione per filtrare il numero dei film
    min_val_sel = st.number_input("Numero di valutazioni minimo per film", 0, step=1, format="%d", key="min_val_sel")

# Aggiorno lo stato se c'è una modifica
if selected_genres3 != st.session_state.selected_genres3:
    st.session_state.selected_genres3 = selected_genres3

# Filtra i dati in base al genere selezionato
Film_rank = Film_rank.filter(pl.col("GenreS") == st.session_state.selected_genres3)

# Filtro eliminando i valori "null" 
Film_rank = Film_rank.filter(
 (pl.col("No of Persons Voted") != "null")  
)

# Convertiamo la colonna "No of Persons Voted" in numerico
Film_rank = Film_rank.with_columns(
    pl.col("No of Persons Voted").str.strip_chars().cast(pl.Int64, strict=False).alias("Votes")
)
# Filtro per il numero di voti
Film_rank = Film_rank.filter(pl.col("Votes") > min_val_sel)

### funzione per filtrare i top 3 film 
def ranker(df):
    top_titles = []
    temp_df = df.clone()  

    while len(top_titles) < 3 and not temp_df.is_empty():
        max_value = temp_df["Rating"].max()  # Trova il valore massimo
        df_max = temp_df.filter(pl.col("Rating") == max_value)
        
        # Prendi il primo titolo tra quelli con il rating massimo
        title_to_add = df_max["Title"].to_list()[0]
        top_titles.append(title_to_add)
        
        # Rimuovi il titolo appena aggiunto
        temp_df = temp_df.filter(pl.col("Title") != title_to_add)

    return top_titles[:3]

# classifico i film per decade con la funzione appena creata
def rankdecade(df):
    decadi = ('1970-1979', '1980-1989', '1990-1999', '2000-2009', '2010-2019', '2020+')
    top_per_decade = {}

    for d in decadi:
        sel = df.filter(pl.col("Decade") == d)
        top_per_decade[d] = ranker(sel)  # Associa la decade ai top 3 film
    return top_per_decade  
top_film = rankdecade(Film_rank)
top_titles = [title for titles in top_film.values() for title in titles]

# Filtro il DataFrame originale per includere solo i top film
filtered_df = Film_rank.filter(pl.col("Title").is_in(top_titles))

# Creazione del grafico
df_1970 = filtered_df.filter((filtered_df['Decade'] == '1970-1979'))
df_1980 = filtered_df.filter((filtered_df['Decade'] == '1980-1989'))
df_1990 = filtered_df.filter((filtered_df['Decade'] == '1990-1999'))
df_2000 = filtered_df.filter((filtered_df['Decade'] == '2000-2009'))
df_2010 = filtered_df.filter((filtered_df['Decade'] == '2010-2019'))
df_2020 = filtered_df.filter((filtered_df['Decade'] == '2020+'))

genre_colors["General"] = "black"

# creo un barplot per ogni decade
chart_1970 = alt.Chart(df_1970).mark_bar().encode(
    x=alt.X('Rating:Q', title='Rating', scale=alt.Scale(domain=[0, 10])),
    y=alt.Y('Title:N', title='Film', sort='-x'),
    color=alt.Color('GenreS:N').scale(
        domain=list(genre_colors.keys()),
        range=list(genre_colors.values())
    ).legend(None)
).properties(
    title="Top Films 1970-1979"
)

chart_1980 = alt.Chart(df_1980).mark_bar().encode(
    x=alt.X('Rating:Q', title='Rating', scale=alt.Scale(domain=[0, 10])),
    y=alt.Y('Title:N', title='Film', sort='-x'),
    color=alt.Color('GenreS:N').scale(
        domain=list(genre_colors.keys()),
        range=list(genre_colors.values())
    ).legend(None)
).properties(
    title="Top Films 1980-1989"
)

chart_1990 = alt.Chart(df_1990).mark_bar().encode(
    x=alt.X('Rating:Q', title='Rating', scale=alt.Scale(domain=[0, 10])),
    y=alt.Y('Title:N', title='Film', sort='-x'),
    color=alt.Color('GenreS:N').scale(
        domain=list(genre_colors.keys()),
        range=list(genre_colors.values())
    ).legend(None)
).properties(
    title="Top Films 1990-1999"
)

chart_2000 = alt.Chart(df_2000).mark_bar().encode(
    x=alt.X('Rating:Q', title='Rating', scale=alt.Scale(domain=[0, 10])),
    y=alt.Y('Title:N', title='Film', sort='-x'),
    color=alt.Color('GenreS:N').scale(
        domain=list(genre_colors.keys()),
        range=list(genre_colors.values())
    ).legend(None)
).properties(
    title="Top Films 2000-2009"
)

chart_2010 = alt.Chart(df_2010).mark_bar().encode(
    x=alt.X('Rating:Q', title='Rating', scale=alt.Scale(domain=[0, 10])),
    y=alt.Y('Title:N', title='Film', sort='-x'),
    color=alt.Color('GenreS:N').scale(
        domain=list(genre_colors.keys()),
        range=list(genre_colors.values())
    ).legend(None)
).properties(
    title="Top Films 2010-2019"
)

chart_2020 = alt.Chart(df_2020).mark_bar().encode(
    x=alt.X('Rating:Q', title='Rating', scale=alt.Scale(domain=[0, 10])),
    y=alt.Y('Title:N', title='Film', sort='-x'),
    color=alt.Color('GenreS:N').scale(
        domain=list(genre_colors.keys()),
        range=list(genre_colors.values())
    ).legend(None)
).properties(
    title="Top Films 2020+"
)

# creo i blocchi per poi printarli
final_chart1 = alt.vconcat(chart_1970, chart_1980,chart_1990,).configure_view(
    continuousWidth=150,
    continuousHeight=100
)

final_chart2 = alt.vconcat(chart_2000,chart_2010, chart_2020).configure_view(
    continuousWidth=150,
    continuousHeight=100
)

col1, col2, col3, col4 = st.columns([1,4,4, 1])
with col2:
    st.altair_chart(final_chart1)
with col3:
    st.altair_chart(final_chart2)

##################################################################
# Best director
##################################################################
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.markdown("<h1 style='font-size: 30px;'>3.2 Migliori Registi dato il Rating</h1", unsafe_allow_html=True)
    st.markdown(""" Questa seconda sezione è fatta per poter esplorare i migliori registi,
                    e scoprire le loro opere. Come prima, abbiamo inserito un filtro per il numero minimo di
                    valutazioni che deve avere un film per essere considerato. In aggiunta è possibile
                filtrare anche il numero minimo di film che deve aver fatto un regista questo 
                per evitare che registi con uno o pochi film risultino in cima 
                a discapito di chi ha avuto un periodo produttivo più longevo.
                """)
    min_val_sel2 = st.number_input("Numero di valutazioni minimo per film", 0, step=1, format="%d",  key="min_val_sel2") 

# Creo uno spazio su cui memorizzare le interazioni
# gli oggetti polars streamlit non li conosce 
# quindi li devo indicizzare con una funzione di hash
# che dato l'oggetto dataframe restituisce il dataframe stesso
# ottengo le coppie title e director uniche non null
@st.cache_data(hash_funcs={"polars.dataframe.frame.DataFrame": lambda df: df.shape})
def get_unique_movies_by_director(dataset):
    unique_movies = dataset.unique(subset=["Title", "Director"])
    return unique_movies.filter(pl.col('Rating').is_not_null())
Film_Registi_Unici = get_unique_movies_by_director(Moviest)

# trasformo "No of Persons Voted" in intero
Film_Registi_Unici = Film_Registi_Unici.with_columns(
    pl.col("No of Persons Voted") 
    .str.strip_chars()
    .cast(pl.Int64, strict=False)
    .alias("Votes")
)
# filtro per il numero di voti
Film_Registi_Unici = Film_Registi_Unici.filter(pl.col("Votes") > min_val_sel2)


# aggiungo hash_funcs anche a questa funzione
# # ottengo media e numero di film
@st.cache_data(hash_funcs={"polars.dataframe.frame.DataFrame": lambda df: df.shape})
def calculate_director_averges(dataset):
    return dataset.group_by('Director').agg(
        pl.col('Rating').mean().alias('Voto_Medio'), 
        pl.col('Title').count().alias('Numero_Film')
    )
Registi_Medie = calculate_director_averges(Film_Registi_Unici)

# numero minimo di film come input
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    number = st.number_input("Numero di film minimo per Regista", 0, 42, step=1, format="%d")
   
# anche qui aggiungo hash_funcs
# filtro per il numero di film minimo
@st.cache_data(hash_funcs={"polars.dataframe.frame.DataFrame": lambda df: df.shape})
def filter_directors_by_film_count(dataset, min_films):
    return dataset.filter(pl.col('Numero_Film') >= min_films)
Registi_Medie = filter_directors_by_film_count(Registi_Medie, number)

# creo la classifica
def best_director(df):
    top_director = []
    dir_df = df.clone()
    
    while len(top_director) < 10 and not dir_df.is_empty():
        max_value = dir_df["Voto_Medio"].max()
        df_max = dir_df.filter(pl.col("Voto_Medio") == max_value)
        
        # estrae il film con raiting massimo
        new_directors = df_max["Director"].to_list()
        top_director.extend(new_directors)
        
        # non considero quelli gia presenti
        dir_df = dir_df.filter(~pl.col("Director").is_in(new_directors))
    
    return top_director[:10] # ritorno i top 10
best_dire = best_director(Registi_Medie)

# aggiungo hash_funcs a questa funzione
# filtra i director nella classifica
@st.cache_data(hash_funcs={"polars.dataframe.frame.DataFrame": lambda df: df.shape})
def filter_top_directors(dataset, director_list):
    return dataset.filter(pl.col("Director").is_in(director_list))
df_best_dire = filter_top_directors(Registi_Medie, best_dire)

# crea la tabella a fianco
def create_director_bar_chart(dataset):
    return alt.Chart(dataset).mark_bar().encode(
        x=alt.X('Voto_Medio:Q', scale=alt.Scale(domain=[0, 10])),
        y=alt.Y("Director:N", sort='-x'),
    ).properties(height=400)
Dir_Chart = create_director_bar_chart(df_best_dire)

col1, col2, col3, col4 = st.columns([1, 5, 3, 1])
with col2:
    st.altair_chart(Dir_Chart)

with col3:
    st.write(df_best_dire.sort("Voto_Medio", descending = True))

# inizializzo lo stato per la selezione del director
if "selected_Director" not in st.session_state:
    st.session_state.selected_Director = None

# ottengo la director list
Registi_selection_box = df_best_dire.get_column("Director").to_list()

# seleziona il regista
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    selected_Director = st.selectbox(
    "Seleziona il Regista:",
    Registi_selection_box,
    index=None)

# classico salvataggio dello stato
if selected_Director != st.session_state.selected_Director:
    st.session_state.selected_Director = selected_Director

# aggiungo hash_funcs a questa funzione
# seleziono il film del director
@st.cache_data(hash_funcs={"polars.dataframe.frame.DataFrame": lambda df: df.shape})
def get_movies_by_director(dataset, director):
    unique_movies = dataset.unique(subset=["Title", "Director"])
    return unique_movies.filter(pl.col("Director") == director)

# se selezionato il director printo la tabella
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    if st.session_state.selected_Director:
        Selected_Dire = get_movies_by_director(Moviest, st.session_state.selected_Director)
        st.write(Selected_Dire.select(["Title", "Genres", "Minutes", "Rating", "No of Persons Voted"]))


##################################################################
############# Algoritmo di ricerca ###############################
##################################################################
# Filtro il dataset originale per ottenere solo i film unici in base a titolo, genere e regista
Film_filtraggio_finale = Moviest.unique(subset=["Title", "GenreS", "Director"])

# Creo una funzione per classificare la durata del film in fasce
def get_durata(minuti):
    try:
        minuti_int = int(minuti)
    except(ValueError, TypeError):
        return 'Unknown'
    
    if minuti_int < 60:
        return '<60'
    elif minuti_int < 90:
        return '60-89'
    elif minuti_int < 120:
        return '90-119'
    elif minuti_int < 150:
        return '120-149'
    else:
        return '150=<'

# Questa funzione restituisce la lista dei generi unici disponibili nel dataset
@st.cache_data
def load_data1000():
    return Film_filtraggio_finale.select(pl.col("GenreS")).unique()["GenreS"]
generi_unici_filtro = load_data1000().to_list()

# testo
col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    st.markdown("<h1 style='font-size: 30px;'>3.3 Ricerca Dei Film</h1>", unsafe_allow_html=True)
    st.markdown("""Quest'ultima parte è dedicata ad un algoritmo di filtraggio dei film, 
                infatti è possibile interrogare direttamente il dataset cercando fil a piacere,
                oppure scoprirne di nuovi in base a genere, decade, regista, minutaggio o
                direttamente con le parole chiave.
                """)

# input titolo
    Title_sel = st.text_input("Titolo", None)

# se non è stato selezionato alcun genere lista vuota
    if "filtro_generi" not in st.session_state:
        st.session_state.filtro_generi = []

# seleziona genere
    Filtro_generi = st.multiselect(
        'Seleziona i generi da visualizzare', 
        generi_unici_filtro, 
        default=st.session_state.filtro_generi
    )
# imposto lo stato della sessione
    st.session_state.filtro_generi = Filtro_generi

# Ragruppo i film dal titolo e aggiungo le colonne descrittive del film
Film_aggregati = Film_filtraggio_finale.group_by(["Title"]).agg(
    pl.col("GenreS").alias("Generi").unique(),
    pl.col("Year").first().alias("Year"),
    pl.col("Rating").first().alias("Avg_Rating"),
    pl.col("Minutes").first().alias("Minutes"), 
    pl.col("Description").first().alias("Description"),
    pl.col("Directed by").first().alias("Directed by"),
)

# applico le classi
Film_aggregati = Film_aggregati.with_columns(
    Decade = pl.col("Year").map_elements(get_decade),
    Durata_Classe = pl.col("Minutes").map_elements(get_durata)
)

# Se è stato inserito un titolo, filtro il dataset in base a quel titolo (ignorando maiuscole/minuscole)
if Title_sel:
    Title_sel_lower = Title_sel.lower()
    Film_aggregati = Film_aggregati.drop_nulls(subset=["Title"])
    Film_aggregati = Film_aggregati.filter(
        pl.col("Title")
        .cast(pl.Utf8)
        .str.to_lowercase()
        .str.contains(Title_sel_lower, literal=True)
    )


#  filtro solo i film che contengono generi selezionati
if Filtro_generi:
    mask = Film_aggregati["Generi"].list.contains(Filtro_generi[0])
    for genere in Filtro_generi[1:]:
        mask &= Film_aggregati["Generi"].list.contains(genere)
    filtered_data_finale = Film_aggregati.filter(mask)
else:
    filtered_data_finale = Film_aggregati

col1, col2, col3 = st.columns([1, 8, 1])

# seleziona la decade
with col2:
    Decade_finale = st.selectbox(
        "Selezionare la Decade",
        ('1970-1979', '1980-1989', '1990-1999', '2000-2009', '2010-2019', '2020+'),
        index=None,
    )

# filtro per la decade
if Decade_finale:
    filtered_data_finale = filtered_data_finale.filter(pl.col("Decade") == Decade_finale)


col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    Director_sel = st.text_input("Regista", None)
# filtro oi registi 
if Director_sel:
    Director_sel_lower = Director_sel.lower() # creo un'altro ds da modificare
    filtered_data_finale = filtered_data_finale.drop_nulls(subset=['Directed by'])
    filtered_data_finale = filtered_data_finale.filter(
        pl.col("Directed by")
        .cast(pl.Utf8)
        .str.to_lowercase()
        .str.contains(Director_sel_lower, literal=True)
    )

col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    Minuti_finale = st.selectbox(
        "Selezionare Minutaggio",
        ('Unknown', '<60', '60-89', '90-119', '120-149', '150=<'),
        index=None,
    )
# filtro il minutaggio 
if Minuti_finale:
    filtered_data_finale = filtered_data_finale.filter(pl.col("Durata_Classe") == Minuti_finale)

col1, col2, col3 = st.columns([1, 8, 1])
with col2:
    keywords_input = st.text_input("Inserisci parole chiave (separate da virgole):")
# filtro per parole chiave
if keywords_input:
    keywords_list = [k.strip().lower() for k in keywords_input.split(",") if k.strip()] # divido in una lista le parole
    filtered_data_finale = filtered_data_finale.drop_nulls(subset=['Description'])
    filtered_data_finale = filtered_data_finale.with_columns(
        pl.col("Description").cast(pl.Utf8).str.to_lowercase()
    )
# filtro ogni parola
    for keyword in keywords_list:
        filtered_data_finale = filtered_data_finale.filter(
            pl.col("Description").str.contains(keyword, literal=True)
        )

col1, col2, col3 = st.columns([1, 8, 1])
# printo tutto
with col2:
    st.write(filtered_data_finale)

