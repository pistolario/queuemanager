CREATE TABLE "colas" ("id" INTEGER PRIMARY KEY  AUTOINCREMENT  NOT NULL  UNIQUE , "nombre" VARCHAR NOT NULL , "descripcion" TEXT NOT NULL , "leyendo" integer);
CREATE TABLE "mensajes" ("id" INTEGER PRIMARY KEY  NOT NULL ,"idcola" INTEGER NOT NULL ,"mensaje" TEXT NOT NULL , "creado" DATETIME, "leido" DATETIME);
CREATE INDEX idx_mensajes_creado on mensajes (creado);
CREATE INDEX idx_mensajes_cola on mensajes (idcola);
CREATE INDEX idx_mensajes_leido on mensajes(leido);
CREATE INDEX idx_vs on mensajes(json_extract(mensaje, "$.VS"));
