#**************************************************************************************#
#Realise par :                                                                         #
#       Khariratou DIALLO                                                              #
#               Danis JIOGUE                                                           #
#                                                                                      #
#Sous la supervision de :                                                              #
#               Dr : Cheikh BA                                                         #
#**************************************************************************************#

#__________Installation des packages sous-jacents
install.packages(c("Rcpp","rjson","RJSONIO","bitops","digest","functional","stringr"
                   ,"plyr"))
install.packages(c("reshape2","dplyr", "R.methodsS3","caTools","Hmisc","bit64",
                   "rJava"))

#___________Installation du package ravro présent dans la machine
install.packages("C:/Users/pc/Downloads/ravro_1.0.4.tar.gz", repos = NULL, 
                 type = "source")

#___________Definition de l'environnement HADOOP
Sys.setenv(HADOOP_CMD="/usr/local/hadoop220/bin/hadoop")
Sys.setenv(HADOOP_STREAMING="/usr/local/hadoop220/share/hadoop/tools/
           lib/hadoop-streaming-2.2.0.jar")
Sys.getenv("HADOOP_CMD")

#___________Installation du package rmr2
install.packages("C:/Users/pc/Downloads/rmr2_3.3.1 (1).tar.gz", repos = NULL, 
                 type = "source")
library(rmr2)
#***Pour utiliser RHADOOP sans HADDOP
rmr.options(backend = "local")


#-------------------------Préparation du jeux de donnee (split & pretraitement)
data <- read.csv(choose.files(),header = T, sep = ";")
data


#-------------------------FUNCTION MAPPER
cle <- "xy"
mapper <- function(null, data){
  Taille_ligne = nrow(data)
  value_X = rep(NA, times = Taille_ligne) #Recupere les observations xi
  value_Y = rep(NA, times = Taille_ligne) #Recupere les observation yi
  cle_M <- list(NA,NA) # Definition du format de la clé en sortie qui est une liste
  valeur_M <- list(rep(NA, times = Taille_ligne),rep(NA, times = Taille_ligne),Taille_ligne)
  for (j in 1:2) {
    if(j==1){  #Nous sommes a la premiere ligne donc les observations sont 
        #relatives a x d'ou la cle = X
      cle_M[[1]] = "X"
      for (i in 1:Taille_ligne) {
        value_X[i] = data[i,j]
        valeur_M[[1]][i] = value_X[i]
      }
    }
    else if(j==2){ #Nous sommes a la deuxieme ligne donc les observations 
      #sont relatives a Y d'ou la cle = Y
      cle_M[[2]] = "Y"
      for (k in 1:Taille_ligne) {
        value_Y[k] = data[k,j]
        valeur_M[[2]][k] = value_Y[k]
      }
    }
  }
  cle <- c("X","Y","n")
  keyval(cle,valeur_M)  #on renvoie une cle est une liste comme valeur
}



#-------------------------FUNCTION REDUCE

#**Defintion des fonctions

#Fonction moyenne de X
moy_x <- function(valeur_M){
  som_x = 0
  for (a in 1:valeur_M[[3]]) {
    som_x = som_x + valeur_M[[1]][a]
  }
  #print(som_x/valeur_M[[3]])
  return(som_x/valeur_M[[3]])
}

#Fonction moyenne de Y
moy_y <- function(valeur_M){
  som_y = 0
  for (b in 1:valeur_M[[3]]) {
    som_y = som_y + valeur_M[[2]][b]
  }
  #print(som_y/valeur_M[[3]])
  return(som_y/valeur_M[[3]])
}

#Fonction variance de X
var_x <- function(valeur_M){
  som_car_x = 0
  for (c in 1:valeur_M[[3]]) {
    som_car_x = som_car_x + valeur_M[[1]][c]*valeur_M[[1]][c]
  }
  #print(som_car_x)
  varia_x = som_car_x/valeur_M[[3]] - moy_x(valeur_M)*moy_x(valeur_M)
  #print(varia_x)
  return(varia_x)
}


#Fonction covarriance de X
cov_xy <- function(valeur_M){
  som_xy = 0
  for (d in 1:valeur_M[[3]]) {
      som_xy = som_xy + valeur_M[[1]][d] * valeur_M[[2]][d]
  }
  #print(som_xy)
  covariance = som_xy/valeur_M[[3]] - moy_y(valeur_M)*moy_x(valeur_M)
  #print(covariance)
  return(covariance)
}

#**** Fonction reduce
reducer <- function(key, val.list){
  a.estim = cov_xy(valeur_M)/var_x(valeur_M)
  b.estim = moy_y(valeur_M) - a.estim*moy_x(valeur_M)
  #print(a.estim);print(b.estim)
  key = c("a","b")
  valeur = c(a.estim, b.estim)
  #print(key); print(valeur)
  keyval(key,valeur)
}
#______________________________________________Appel de la foction MapReduce

data_BD <- to.dfs(data)
lienar <- mapreduce(input=data_BD,
          map=mapper,
          reduce = reducer
)
result <- from.dfs(lienar)
result$key[1];result$key[4];result$val[1];result$val[4]
