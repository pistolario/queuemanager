#!python2
# vim: set fileencoding=utf-8 :
# -*- coding: utf-8 -*-
#
import os
import os.path
import SocketServer
import threading
# import socket
import csv
from optparse import OptionParser
import sqlite3
import sys
import codecs
import getch
import datetime
import readline
import json
import cStringIO
import time
import csvunicode


VERSION = "1.1"
TAMANO_BLOQUE_LOAD = 500


class EmptyQueueException(Exception):
    def __init__(self, msg):
        super(EmptyQueueException, self).__init__(msg)


def logger(f, name=None):
    # if logger.fhwr isn't defined and open ...
    try:
        if logger.fhwr:
            pass
    except:
        # ... open it
        # logger.fhwr = open("que","w")
        logger.fhwr = sys.stdout
    if name is None:
        name = f.func_name

        def wrapped(*args, **kwargs):
                logger.fhwr.write("*** "+name+" "+str(f)+"\n" +
                                str(args) + str(kwargs) + "\n\n")
                result = f(*args, **kwargs)
                logger.fhwr.write("*** "+name+" FIN\n")
                return result
        wrapped.__doc__ = f.__doc__
        return wrapped


class CGestorColas:
    def __init__(self, sfich):
        self.sfich = sfich
        self.db = sqlite3.connect(sfich, isolation_level='DEFERRED',
                check_same_thread=False)
        self.lock = threading.Lock()
        self.paused = {}

    def pushMessage(self, idcola, mensaje):
        self.lock.acquire()
        try:
            if not self._existsQueue(idcola):
                raise Exception("Error, cola no existe")
            self._insertaMensaje(idcola, mensaje)
            self.db.commit()
        finally:
            self.lock.release()

    def mpushMessage(self, idcola, mensajes):
        self.lock.acquire()
        try:
            if not self._existsQueue(idcola):
                raise Exception("Error, cola no existe")
            tmsg=[]
            for mensaje in mensajes:
                tmsg.append((mensaje, datetime.datetime.now()))

            self._insertaMensajes(idcola, tmsg)
            self.db.commit()
        finally:
            self.lock.release()

    def pauseQueue(self, idcola):
        self.paused[idcola]=1

    def unpauseQueue(self, idcola):
        if idcola in self.paused:
            del self.paused[idcola]

    def isPausedQueue(self, idcola):
        if idcola in self.paused:
            return True
        else:
            return False

    def pollMessage(self, idcola):
        if self.isPausedQueue(idcola):
            return (0, None)
        self.lock.acquire()
        try:
            if not self._existsQueue(idcola):
                raise Exception("Error, cola no existe")
            (idmsg, datos) = self._leeMensaje(idcola)
            self.db.commit()
            if idmsg is not None:
                return (1, datos)
        except EmptyQueueException:
            return (0, None)
        finally:
            self.lock.release()
        return (0, None)

    def jumpQueue(self, idcola, cuanto):
        self.lock.acquire()
        try:
            if not self._existsQueue(idcola):
                raise Exception("Error, cola no existe")
            if cuanto > 0:
                (idmsg, datos) = self._jumpMensaje(idcola, cuanto)
            else:
                (idmsg, datos) = self._leeMensaje(idcola)
            if idmsg is not None:
                return (1, datos)
        except EmptyQueueException:
            return (0, None)
        finally:
            self.lock.release()
        return (0, None)

    def backQueue(self, idcola, cuanto):
        self.lock.acquire()
        try:
            if not self._existsQueue(idcola):
                raise Exception("Error, cola no existe")
            (idmsg, datos) = self._backMensaje(idcola, cuanto)
            if idmsg is not None:
                return (1, datos)
        except EmptyQueueException:
            return (0, None)
        finally:
            self.lock.release()
        return (0, None)

    def undoQueue(self, idcola, cuanto):
        self.lock.acquire()
        try:
            if not self._existsQueue(idcola):
                raise Exception("Error, cola no existe")
            self._undoQueue(idcola, cuanto)
        finally:
            self.lock.release()

    def popMessage(self, idcola):
        if self.isPausedQueue(idcola):
            return None
        self.lock.acquire()
        try:
            if not self._existsQueue(idcola):
                raise Exception("Error, cola no existe")
            (idmsg, datos) = self._leeMensaje(idcola)
            if datos is not None:
                self._daPorLeidoMensaje(idmsg)
                self.db.commit()
            return datos
        except EmptyQueueException:
            return None
        finally:
            self.lock.release()
        return None

    def mpopMessage(self, idcolas):
        idcolas2 = []
        for idcola in idcolas:
            if not self.isPausedQueue(idcola):
                idcolas2.append(idcola)
        if len(idcolas2) == 0:
            return (-1, None)
        self.lock.acquire()
        try:
            for idcola in idcolas2:
                if not self._existsQueue(idcola):
                    raise Exception("Error, cola "+idcola+" no existe")
            (idmsg, idcolaresp, datos) = self._leeMensajeM(idcolas2)
            if datos is not None:
                self._daPorLeidoMensaje(idmsg)
                self.db.commit()
            return (idcolaresp, datos)
        except EmptyQueueException:
            return (-1, None)
        finally:
            self.lock.release()
        return None

    def emptyQueue(self, idcola):
        self.lock.acquire()
        try:
            c = self.db.cursor()
            c.execute("delete from mensajes WHERE idcola=:idcola", {"idcola": idcola})
            self.db.commit()
        finally:
            self.lock.release()

    def unreadQueue(self, idcola):
        self.lock.acquire()
        try:
            c = self.db.cursor()
            c.execute("update mensajes set leido=null WHERE idcola=:idcola", {"idcola": idcola})
            self.db.commit()
        finally:
            self.lock.release()

    def _undoQueue(self, idcola, cuanto):
        c = self.db.cursor()
        c.execute("update mensajes set leido=null WHERE rowid in ( select rowid from mensajes where idcola=:idcola order by leido DESC limit :cuanto )", {"idcola": idcola, "cuanto": cuanto})
        self.db.commit()

    def emptyAllQueues(self):
        self.lock.acquire()
        try:
            c = self.db.cursor()
            c.execute("delete from mensajes")
            self.db.commit()
        finally:
            self.lock.release()

    def getState(self, idcola):
        self.lock.acquire()
        try:
            res = {}
            c = self.db.cursor()
            c.execute("select count(id) from mensajes where idcola=:idcola and leido is null", {"idcola": idcola})
            f = c.fetchone()
            if f is None:
                raise Exception("No existe mensaje para dicha cola")
            res["pending"] = f[0]
            c.execute("select count(id) from mensajes where idcola=:idcola", {"idcola": idcola})
            f = c.fetchone()
            if f is None:
                raise Exception("No existe mensaje para dicha cola")
            res["total"] = f[0]
            c.execute("select count(id) from mensajes where idcola=:idcola and leido is not null", {"idcola": idcola})
            f = c.fetchone()
            if f is None:
                raise Exception("No existe mensaje para dicha cola")
            res["read"] = f[0]
            if self.isPausedQueue(idcola):
                res["paused"] = "True"
            else:
                res["paused"] = "False"
            return res
        finally:
            self.lock.release()

    def listQueues(self):
        self.lock.acquire()
        try:
            queues = []
            c = self.db.cursor()
            c.execute("select id, nombre, descripcion from colas order by id asc")
            res = c.fetchall()
            for f in res:
                queues.append({"id": f[0], "nombre": f[1], "desc": f[2]})
            return queues
        finally:
            self.lock.release()

    def createQueue(self, nombre, descripcion):
        self.lock.acquire()
        try:
            c = self.db.cursor()
            c.execute("insert into colas (nombre, descripcion) values (:nombre, :descripcion)", {"nombre": nombre, "descripcion": descripcion})
            lastId = c.lastrowid
            self.db.commit()
            return lastId
        finally:
            self.lock.release()

    def deleteQueue(self, idQueue):
        self.emptyQueue(idQueue)
        self.lock.acquire()
        try:
            c = self.db.cursor()
            c.execute("delete from colas where id=:idqueue", {"idqueue": idQueue})
            self.db.commit()
        finally:
            self.lock.release()

    def _daPorLeidoMensaje(self, idmsg):
        c = self.db.cursor()
        c.execute("update mensajes set leido=:ahora where id=:idmsg", {"ahora": datetime.datetime.now(), "idmsg": idmsg})

    def _eliminaMensaje(self, idmsg):
        c = self.db.cursor()
        c.execute("delete from mensajes where id=:idmsg", {"idmsg": idmsg})

    def _insertaMensaje(self, idcola, mensaje):
        c = self.db.cursor()
        c.execute("insert into mensajes (idcola, mensaje, creado) values (:idcola, :mensaje, :creado)", {"idcola": idcola, "mensaje": mensaje, "creado": datetime.datetime.now()})
        # lastId = c.lastrowid
        self.db.commit()

    def _insertaMensajes(self, idcola, mensajes):
        c = self.db.cursor()
        c.executemany("insert into mensajes (idcola, mensaje, creado) values ({0}, ?, ?)".format(idcola), mensajes)
        self.db.commit()

    def _existsQueue(self, idcola):
        c = self.db.cursor()
        c.execute("select id from colas where id=:idcola", {"idcola": idcola})
        f = c.fetchone()
        if f is None:
            return False
        else:
            return True

    def _leeMensaje(self, idcola):
        c = self.db.cursor()
        c.execute("select id, mensaje from mensajes where idcola=:idcola and leido is null order by creado asc, id asc limit 1", {"idcola": idcola})
        f = c.fetchone()
        if f is None:
            raise EmptyQueueException("No existe mensaje para dicha cola")
        return (f[0], f[1])

    def _jumpMensaje(self, idcola, cuanto):
        c=self.db.cursor()
        c.execute("select id, mensaje from mensajes where idcola=:idcola and leido is null order by creado asc, id asc limit 1 offset {0}".format(cuanto), {"idcola": idcola})
        f=c.fetchone()
        if f is None:
            raise EmptyQueueException("No existe mensaje para dicha cola")
        return (f[0], f[1])

    def _backMensaje(self, idcola, cuanto):
        c=self.db.cursor()
        c.execute("select id, mensaje from mensajes where idcola=:idcola and leido is not null order by leido DESC, id DESC limit 1 offset {0}".format(cuanto), {"idcola": idcola})
        f=c.fetchone()
        if f == None:
            raise EmptyQueueException("No existe mensaje para dicha cola")
        return (f[0], f[1])


    def _leeMensajeM(self, idcolas):
        c=self.db.cursor()
        v=",".join(map(str, idcolas))
        c.execute("select id, idcola, mensaje from mensajes where idcola IN (" + v + ") and leido is null order by creado asc, id asc limit 1")
        f=c.fetchone()
        if f == None:
            raise EmptyQueueException("No existe mensaje para dicha cola")
        return (f[0], f[1], f[2])


"""Se construye un objeto para cada petición
"""
class ReceptorColaMensajes(SocketServer.StreamRequestHandler):
    gestorColas=None
    def handle(self):
        # self.rfile is a file-like object created by the handler;
        # we can now use e.g. readline() instead of raw recv() calls
        cabecera = self.rfile.readline().strip()
        if cabecera.startswith("PUSH"):
            self.handlePushMessage(cabecera)
        elif cabecera.startswith("POLL"):
            self.handlePollMessage(cabecera)
        elif cabecera.startswith("POP"):
            self.handlePopMessage(cabecera)
        elif cabecera.startswith("MPOP"):
            self.handleMPopMessage(cabecera)
        else:
            self.wfile.write("Error, cabecera no reconocida {0}".format(cabecera))
        self.rfile.close()
        self.wfile.close()
        # Likewise, self.wfile is a file-like object used to write back
        # to the client
    def handlePushMessage(self, cabecera):
        try:
            tokens=cabecera.split()
            scola=tokens[1]
            idcola=int(scola)
            datos=""
            linea=self.rfile.readline()
            linea=linea.rstrip("\r\n")
            if linea.startswith(">"):
                linea=linea[1:]
            while linea!=None and not linea.startswith("MSGEND"):
                if len(datos)>0:
                    datos=datos+"\n"+linea
                else:
                    datos=linea
                linea=self.rfile.readline()
                if linea!=None:
                    linea=linea.rstrip("\r\n")
                    if linea.startswith(">"):
                        linea=linea[1:]
            res=unicode(datos, "utf-8")# res es object Unicode
            #Aqui ya tenemos cola y mensaje
            ReceptorColaMensajes.gestorColas.pushMessage(idcola, res)
        except Exception as e:
            print("Exception: {0}".format(e))
            self.wfile.write("Error: {0}".format(e))

    def handlePollMessage(self, cabecera):
        try:
            tokens=cabecera.split()
            scola=tokens[1]
            idcola=int(scola)
            (n, primero)=ReceptorColaMensajes.gestorColas.pollMessage(idcola)
            if n!=0:
                self.sendContentMessage(primero)
            else:
                self.wfile.write("NONE")
        except Exception as e:
            print("Exception: {0}".format(e))
            self.wfile.write("Error: {0}".format(e))

    def handleMPopMessage(self, cabecera):
        try:
            tokens=cabecera.split()
            colas=[]
            for scola in tokens[1:]:
                idcola=int(scola)
                colas.append(idcola)
            (idcolaresp, primero)=ReceptorColaMensajes.gestorColas.mpopMessage(colas)
            if primero is not None:
                self.sendContentMessageN(idcolaresp, primero)
            else:
                self.wfile.write("NONE")
        except Exception as e:
            print("Exception: {0}".format(e))
            self.wfile.write("Error: {0}".format(e))

    def handlePopMessage(self, cabecera):
        try:
            tokens=cabecera.split()
            scola=tokens[1]
            idcola=int(scola)
            mensaje=ReceptorColaMensajes.gestorColas.popMessage(idcola)
            if mensaje!=None:
                self.sendContentMessage(mensaje)
            else:
                self.wfile.write("NONE")
        except Exception as e:
            print("Exception: {0}".format(e))
            self.wfile.write("Error: {0}".format(e))

    #data es un unicode
    def sendContentMessageN(self, idcola, data):
        self.wfile.write("MSG")
        self.wfile.write(" ")
        self.wfile.write(str(idcola))
        cadena=data.encode("utf-8")
        lineas=cadena.split("\n")
        for linea in lineas:
            self.wfile.write("\n")
            if linea.startswith(">"):
                self.wfile.write(">")
            elif linea.startswith("MSG"):
                self.wfile.write(">")
            self.wfile.write(linea)
        self.wfile.write("\nMSGEND")

    #data es un unicode
    def sendContentMessage(self, data):
        self.wfile.write("MSG")
        cadena=data.encode("utf-8")
        lineas=cadena.split("\n")
        for linea in lineas:
            self.wfile.write("\n")
            if linea.startswith(">"):
                self.wfile.write(">")
            elif linea.startswith("MSG"):
                self.wfile.write(">")
            self.wfile.write(linea)
        self.wfile.write("\nMSGEND")

class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
        pass

"""Se construye un objeto para cada peticion
"""
class ReceptorConsola(SocketServer.StreamRequestHandler):
    gestorColas=None
    password=None
    serverColas=None
    serverConsola=None
    def handle(self):
        self.wfile.write("Greetings from QueueManager V.{0}\r\n".format(VERSION))
        self.wfile.write("Password: ")
        maxTries=3
        autorizado=False
        while maxTries>0 and not autorizado:
            entrada=self.rfile.readline().strip()
            if entrada==ReceptorConsola.password:
                autorizado=True
            else:
                self.wfile.write("Wrong. Try again: ")
                maxTries-=1
        if not autorizado:
            return
        self.wfile.write("OK\r\n")
        finBucle=False
        while not finBucle:
            entrada = self.rfile.readline().strip()
            comando=entrada[0:3]
            comando=comando.upper()
            if comando.startswith("END"):
                ReceptorConsola.serverColas.shutdown()
                self.wfile.write("Terminating\r\n...")
                ReceptorConsola.serverConsola.shutdown()
                self.rfile.close()
                self.wfile.close()
                finBucle=True
            else:
                salida=interpretaEntrada(self.gestorColas, entrada)
                if isinstance(salida, basestring):
                    self.wfile.write(salida.replace("\n", "\r\n"))
                    self.wfile.write("\r\n")
                else:
                    finBucle=salida

def completer(text, state):
    options = [x for x in addrs if x.startswith(text)]
    try:
        return options[state]
    except IndexError:
        return None

def completaSalida(entrada, extra):
    if len(entrada)>0:
        entrada+="\n"
    return entrada+extra

def ejecutaComandoState(gestor, params):
    salida=""
    lparams=params.split(" ")
    if len(lparams)==0:
        salida=completaSalida(salida,"Error, no queue suplied")
    else:
        try:
            q=int(lparams[0])
        except:
            salida=completaSalida(salida, "Error, queue not a number")
            return salida
        res=gestor.getState(q)
        salida=completaSalida(salida, "Queue {0}: TOTAL {1}, READ {2}, PENDING: {3}, Paused: {4}".format(q, res["total"], res["read"], res["pending"], res["paused"]))
    return salida

def ejecutaComandoNext(gestor, params):
    salida=""
    lparams=params.split(" ")
    if len(lparams)==0:
        salida=completaSalida(salida,"Error, no queue suplied")
    elif len(lparams)<2:
        salida=completaSalida(salida, "Error, no jump messages given")
    else:
        try:
            q=int(lparams[0])
        except:
            salida=completaSalida(salida, "Error, queue not a number")
            return salida
        try:
            salto=int(lparams[1])
        except:
            salida=completaSalida(salida, "Error, Jump not a number")
            return salida
        if salto<0:
            salida=completaSalida(salida, "Error, jump out of range [0,]")

        res=gestor.jumpQueue(q, salto)
        if res[0]==0:
            salida=completaSalida(salida, "No encontrados")
        else:
            salida=completaSalida(salida, repr(res[1][:4000]))
    return salida

def ejecutaComandoLast(gestor, params):
    salida=""
    lparams=params.split(" ")
    if len(lparams)==0:
        salida=completaSalida(salida,"Error, no queue suplied")
    elif len(lparams)<2:
        salida=completaSalida(salida, "Error, no last messages given")
    else:
        try:
            q=int(lparams[0])
        except:
            salida=completaSalida(salida, "Error, queue not a number")
            return salida
        try:
            salto=int(lparams[1])
        except:
            salida=completaSalida(salida, "Error, back not a number")
            return salida
        if salto<0:
            salida=completaSalida(salida, "Error, last out of range [0, ]")

        res=gestor.backQueue(q, salto)
        if res[0]==0:
            salida=completaSalida(salida, "No encontrados")
        else:
            salida=completaSalida(salida, repr(res[1][:4000]))
    return salida

def ejecutaComandoUndo(gestor, params):
    salida=""
    lparams=params.split(" ")
    if len(lparams)==0:
        salida=completaSalida(salida,"Error, no queue suplied")
    elif len(lparams)<2:
        salida=completaSalida(salida, "Error, no undo messages given")
    else:
        try:
            q=int(lparams[0])
        except:
            salida=completaSalida(salida, "Error, queue not a number")
            return salida
        try:
            salto=int(lparams[1])
        except:
            salida=completaSalida(salida, "Error, undo not a number")
            return salida
        if salto<1:
            salida=completaSalida(salida, "Error, undo out of range [1, ]")

        gestor.undoQueue(q, salto)
        salida=completaSalida(salida, "Undo step successfull")
    return salida


def ejecutaComandoPause(gestor, params):
    salida=""
    lparams=params.split(" ")
    if len(lparams)==0:
        salida=completaSalida(salida,"Error, no queue suplied")
    else:
        try:
            q=int(lparams[0])
        except:
            salida=completaSalida(salida, "Error, queue not a number")
            return salida
        res=gestor.pauseQueue(q)
        salida=completaSalida(salida, "Queue {0} paused".format(q))
    return salida

def ejecutaComandoUnpause(gestor, params):
    salida=""
    lparams=params.split(" ")
    if len(lparams)==0:
        salida=completaSalida(salida,"Error, no queue suplied")
    else:
        try:
            q=int(lparams[0])
        except:
            salida=completaSalida(salida, "Error, queue not a number")
            return salida
        res=gestor.unpauseQueue(q)
        salida=completaSalida(salida, "Queue {0} unpaused".format(q))
    return salida

def ejecutaComandoList(gestor, params):
    """Ignoramos los parametros"""
    salida=""
    lista=gestor.listQueues()
    for l in lista:
        salida=completaSalida(salida,repr(u"[{0}]: {1} -> {2}".format(l["id"], l["nombre"], l["desc"])))
        #salida=completaSalida(salida, "\n")
    return salida

def ejecutaComandoEmpty(gestor, params):
    salida=""
    lparams=params.split(" ")
    if len(lparams)==0:
        salida=completaSalida(salida, "Error, no queue suplied")
    for sQ in lparams:
        if sQ.upper()=="ALL":
            gestor.emptyAllQueues()
            salida=completaSalida(salida, "Flushed all queues")
            return salida
        else:
            try:
                q=int(sQ)
            except:
                salida=completaSalida(salida, "Error, queue {0} not a number".format(sQ))
                return salida
            gestor.emptyQueue(q)
            salida=completaSalida(salida, "Flushed queue {0}".format(q))
    return salida

def ejecutaComandoUnread(gestor, params):
    salida=""
    lparams=params.split(" ")
    if len(lparams)==0:
        salida=completaSalida(salida, "Error, no queue suplied")
    for sQ in lparams:
        try:
            q=int(sQ)
        except:
            salida=completaSalida(salida, "Error, queue {0} not a number".format(sQ))
            return salida
        gestor.unreadQueue(q)
        salida=completaSalida(salida, "Flushed queue {0}".format(q))
    return salida

def ejecutaComandoPoll(gestor, params):
    salida=""
    lparams=params.split(" ")
    if len(lparams)==0:
        salida=completaSalida(salida, "Error, no queue suplied")
    else:
        try:
            q=int(lparams[0])
        except:
            salida=completaSalida(salida, "Error, queue not a number")
            return salida
        try:
            res=gestor.pollMessage(q)
            if res==None or res[0]==0:
                salida=completaSalida(salida, "No message pending in queue {0}".format(q))
            else:
                salida=completaSalida(salida, repr(res[1][:4000]))
        except Exception as e:
            salida=completaSalida(salida, "No message pending in queue {0}, {1}".format(q, e))
    return salida

def ejecutaComandoRecv(gestor, params):
    lparams=params.split(" ")
    if len(lparams)==0:
        return "Error, no queue suplied"
    else:
        try:
            q=int(lparams[0])
        except:
            return "Error, queue not a number"
        try:
            res=gestor.popMessage(q)
            if res==None:
                return "No message pending in queue {0}".format(q)
            else:
                return repr(res[1:2000])
        except Exception as e:
            return "No message pending in queue {0}, {1}".format(q, e)

def ejecutaComandoSend(gestor, params):
    posSeparador=params.find(" ")
    if posSeparador<0:
        return "Error, no queue supplied"
    lparams=[]
    lparams.append(params[0:posSeparador])
    lparams.append(params[posSeparador+1:])
    try:
        q=int(lparams[0])
    except:
        return "Error, queue not a number"
    if len(lparams)==1:
        return "Error, no message supplied"
    else:
        content=lparams[1]
        uContent=eval("u\"\"\""+content+"\"\"\"")
        gestor.pushMessage(q, uContent)
        return "Message sent!"

def ejecutaComandoAdd(gestor, params):
    mockFile=cStringIO.StringIO(params)
    mockCSV=csv.reader(mockFile, delimiter=',')
    try:
        row=mockCSV.next()
    except:
        return "Problemas leyendo parametros. 2 arguments ',' delimited must be supplied"
    if len(row)!=2:
        return "Error, 2 arguments have to be supplied"
    nombre=unicode(eval('"'+row[0].replace('"', '\\"')+'"'), "utf-8")
    descripcion=unicode(eval('"'+row[1].replace('"', '\\"')+'"'), "utf-8")
    q=gestor.createQueue(nombre, descripcion)
    if q!=None:
        return "Created queue {0}".format(q)

def ejecutaComandoDelete(gestor, params):
    lparams=params.split(" ")
    if len(lparams)==0:
        return "Error, no queue suplied"
    else:
        try:
            q=int(lparams[0])
        except:
            return "Error, queue not a number"
        gestor.deleteQueue(q)

INFO_AYUDA="""Comandos disponibles:
    EXIT                 salir
    HELP                 ayuda
    STATUS cola    Estado de mensajes en una cola
    LIST                 Muestra informacion basica de TODAS las colas en el sistema
    EMPTY cola     Vacia una o todas las colas (ALL)
    POLL cola        Muestra el primer mensaje en la cola, sin consumirlo
    NEXT cola cuanto        Muestra el siguiente mensaje tras varios la cola, sin consumirlo, ni ser afectado por pausa
    RECV cola        Consume el primer mensaje en la cola
    SEND cola        mensaje Introduce mensaje en la cola. Dicho mensaje admite codigos
    ADD    nombre,descripcion Crea una cola de mensajes, con codigos UTF8
    DEL cola         Borra cola
    PAUSE cola     Hace que una cola deje de dar mensajes pendientes
    PLAY  cola Hace que una cola vuelva a dar mensajes pendientes
    UNREAD cola     Deshace la lectura de todos los mensajes de una cola
    LAST cola cuanto    Muestra los ultimos leidos de una cola. Cuanto indica 0 -> ultimo, 1 -> otro atras,...
    UNDO cola cuanto     Deshace la lectura de 'cuantos' mensajes de una cola
    LOAD cola fila_inicial  [columna-nombre]+ from nombre_archivo Archivo CSV para cargar como mensajes a cola
    RAWLD cola fila_inicial nombre_archivo Archivo CSV para cargar como mensajes a cola
    DELAY %Y-%m-%dT%H:%M comando
 """.replace("\n", "\r\n")
def muestraAyuda():
    return INFO_AYUDA

def ejecutaComandoLoad(gestor, params):
    posCorte=params.find(" ")
    if posCorte<0:
        return "Error, no queue row and file supplied"
    else:
        sQueue=params[0:posCorte]
        try:
            queue=int(sQueue)
        except:
            return "Error, queue not a number {0}".format(sQueue)
    print("Cola: "+repr(queue))
    params=params[posCorte+1:]
    #Starting row
    posCorte=params.find(" ")
    if posCorte<0:
        return "Error, no row supplied"
    else:
        sRow=params[0:posCorte]
        try:
            row=int(sRow)
        except:
            return "Error, row not a number {0}".format(sRow)

    print("Row: "+repr(row))
    params=params[posCorte+1:]
    #Pairs column-name
    column_pairs={}
    posCorte=params.find(" ")
    while posCorte>=0:
        dscr = params[0:posCorte]
        if dscr == "from":
            break
        if len(dscr)>0:
            pair = dscr.split("-")
            if len(pair)!=2:
                return "Error, pair format error: "+dscr
            sColumn=pair[0]
            try:
                column = int(sColumn)
            except:
                return "Error, row not a number {0}".format(sRow)
            column_pairs[pair[1]]=column
            print("Par {0}-{1}", column, pair[1])
        params=params[posCorte+1:]
        posCorte=params.find(" ")

    if len(column_pairs)==0:
        return "Error, no column pair supplied"
    if posCorte<0:
        return "Error, no file supplied"
    else:
        params=params[posCorte+1:]
        posCorte=params.find(" ")
        while posCorte==0:
            params=params[posCorte+1:]
            posCorte=params.find(" ")

        sFich=params
        if sFich[0]=='"':
            if sFich[-1]=='"':
                sFich=sFich[1:len(sFich)-1]
            else:
                return "Error in file supplied: {0}".format(sFich)
    if not os.path.exists(sFich):
        return "Error, file doesn't exist"

    #CSV loading
    fichcsv=csvunicode.UnicodeReader(file(sFich), dialect="excel", delimiter=";")
    cargados=0
    lleva=0
    bloque=[]
    for linea in fichcsv:
        if lleva>=row:
            msg = {}
            for key, position in column_pairs.iteritems():
                if len(linea)>position:
                    msg[key]=linea[position]
            bloque.append(json.dumps(msg))
            if len(bloque)>TAMANO_BLOQUE_LOAD:
                gestor.mpushMessage(queue, bloque)
                bloque=[]
            cargados+=1
        lleva+=1
    if len(bloque)>0:
        gestor.mpushMessage(queue, bloque)
    return "Messages loaded / sent: {0} / {1}".format(lleva, cargados)

def ejecutaComandoLoadRaw(gestor, params):
    posCorte=params.find(" ")
    if posCorte<0:
        return "Error, no queue column and file supplied"
    else:
        sQueue=params[0:posCorte]
        try:
            queue=int(sQueue)
        except:
            return "Error, queue not a number {0}".format(sQueue)

    params=params[posCorte+1:]
    posCorte=params.find(" ")
    if posCorte<0:
        return "Error, no row supplied"
    else:
        sRow=params[0:posCorte]
        try:
            row=int(sRow)
        except:
            return "Error, row not a number {0}".format(sRow)
        sFich=params[posCorte+1:]
        if sFich[0]=='"':
            if sFich[-1]=='"':
                sFich=sFich[1:len(sFich)-1]
            else:
                return "Error in file supplied: {0}".format(sFich)
        #Ambos parametros estan OK conseguidos
    if not os.path.exists(sFich):
        return "Error, file doesn't exist"
    fich=codecs.open(sFich, "r", "utf-8")
    cargados=0
    lleva=0
    bloque=[]
    for linea in fich:
        if lleva>=row:
            linea=linea.strip()
            bloque.append(linea)
            if len(bloque)>=TAMANO_BLOQUE_LOAD:
                gestor.mpushMessage(queue, bloque)
                bloque=[]
            cargados+=1
        lleva+=1
    if len(bloque)>0:
        gestor.mpushMessage(queue, bloque)
    return "Messages loaded / sent: {0} / {1}".format(lleva, cargados)

FORMATOFECHA="%Y-%m-%dT%H:%M"

def ejecutaComandoDelay(gestor, params):
    posCorte=params.find(" ")
    if posCorte<0:
        return "Error, no time supplied"
    else:
        sCuando=params[0:posCorte]
        try:
            cuando=datetime.datetime.strptime(sCuando, FORMATOFECHA)
        except ValueError as e:
            return "Error, time not understood <{0}>".format(sCuando)

    resto=params[posCorte+1:]
    delayManager.anadeDelayed(cuando, resto)
    return "Planificado comando [{0}] para: {1}".format(resto, cuando)


def interpretaEntrada(gestor, entrada):
    comando=entrada[0:6]
    comando=comando.upper()
    salida=""
    if comando.startswith("EXIT"):
        return True
    elif comando.startswith("HELP"):
        salida=muestraAyuda()
    elif comando.startswith("STATUS"):
        params=entrada[6:]
        params=params.strip()
        salida=ejecutaComandoState(gestor, params)
    elif comando.startswith("NEXT"):
        params=entrada[4:]
        params=params.strip()
        salida=ejecutaComandoNext(gestor, params)
    elif comando.startswith("UNDO"):
        params=entrada[4:]
        params=params.strip()
        salida=ejecutaComandoUndo(gestor, params)
    elif comando.startswith("LAST"):
        params=entrada[4:]
        params=params.strip()
        salida=ejecutaComandoLast(gestor, params)
    elif comando.startswith("PAUSE"):
        params=entrada[5:]
        params=params.strip()
        salida=ejecutaComandoPause(gestor, params)
    elif comando.startswith("PLAY"):
        params=entrada[4:]
        params=params.strip()
        salida=ejecutaComandoUnpause(gestor, params)
    elif comando.startswith("LIST"):
        params=entrada[4:]
        params=params.strip()
        salida=ejecutaComandoList(gestor, params)
    elif comando.startswith("EMPTY"):
        params=entrada[5:]
        params=params.strip()
        salida=ejecutaComandoEmpty(gestor, params)
    elif comando.startswith("UNREAD"):
        params=entrada[6:]
        params=params.strip()
        salida=ejecutaComandoUnread(gestor, params)
    elif comando.startswith("POLL"):
        params=entrada[4:]
        params=params.strip()
        salida=ejecutaComandoPoll(gestor, params)
    elif comando.startswith("RECV"):
        params=entrada[4:]
        params=params.strip()
        salida=ejecutaComandoRecv(gestor, params)
    elif comando.startswith("SEND"):
        params=entrada[4:]
        params=params.strip()
        salida=ejecutaComandoSend(gestor, params)
    elif comando.startswith("ADD"):
        params=entrada[3:]
        params=params.strip()
        salida=ejecutaComandoAdd(gestor, params)
    elif comando.startswith("DELAY"):
        params=entrada[5:]
        params=params.strip()
        salida=ejecutaComandoDelay(gestor, params)
    elif comando.startswith("DEL"):
        params=entrada[3:]
        params=params.strip()
        salida=ejecutaComandoDelete(gestor, params)
    elif comando.startswith("RAWLD"):
        params=entrada[5:]
        params=params.strip()
        salida=ejecutaComandoLoadRaw(gestor, params)
    elif comando.startswith("LOAD"):
        params=entrada[4:]
        params=params.strip()
        salida=ejecutaComandoLoad(gestor, params)
    elif len(comando)==0:
        salida=""
    else:
        salida="Command unknown. You entered {0}".format(entrada)
    return salida

class DelayManager(object):
    def __init__(self, gestor):
        """
        """
        self.lockMonitor=threading.RLock()
        self.terminar=False
        self.tablaDelayed={}
        self.gestor=gestor
        self.hiloMonitor=threading.Thread(group=None, target=self.hilo)
        self.hiloMonitor.start()
    def anadeDelayed(self, cuando, comando):
        with self.lockMonitor:
            if cuando in self.tablaDelayed.keys():
                self.tablaDelayed[cuando].append(comando)
            else:
                self.tablaDelayed[cuando]=[comando,]

    def ejecutaComandosTocan(self):
        nuevaTabla={}
        ahora=datetime.datetime.now()
        with self.lockMonitor:
            if len(self.tablaDelayed.keys())<0:
                return
            for cuando, comandos in self.tablaDelayed.iteritems():
                if cuando<ahora:
                    for comando in comandos:
                        try:
                            salida=interpretaEntrada(self.gestor, comando)
                            print("Resultado: {0}".format(salida))
                        except Exception as e:
                            print("Error llamando a interpretarEntrada: {0}".foramt(e))
                else:
                    nuevaTabla[cuando]=comandos
            self.tablaDelayed=nuevaTabla



    def indicaTocaTerminar(self, terminar):
        with self.lockMonitor:
            self.terminar=terminar

    def tocaTerminar(self):
        with self.lockMonitor:
            return self.terminar

    def hilo(self):
        i=0
        while not self.tocaTerminar():
            i=i+1
            if i%60==0:
                self.ejecutaComandosTocan()
                i=0
            else:
                time.sleep(1.0)

delayManager=None

if __name__ == "__main__":
    HOST, PORT = "localhost", 9999
    DirRaizAplicacion=os.path.dirname(os.path.abspath(__file__))
    print("Queue Manager V. {version}".format(version=VERSION))
    sys.stdout=codecs.EncodedFile(sys.stdout, "latin1", "latin1")
    strUsoComando = "Queue Manager V. {version}".format(version=VERSION)
    parser=OptionParser(usage=strUsoComando, version=("%%prog V. %s" % VERSION))
    parser.add_option("-s", "--host", dest="host", default="127.0.0.1",
        help="Servidor desde el que funcionar")
    parser.add_option("-p", "--port", dest="port", type="int", default=9999,
        help="Puerto que usar")
    parser.add_option("-b", "--db", dest="sfichDB", default=os.path.join(DirRaizAplicacion, "colamensajes.db"),
        help="Fichero base de datos")
    parser.add_option("-d", "--del", dest="deleteQueue", action="append",
            help="Queue to delete")
    parser.add_option("-i", "--interactive", dest="interactive",
        action="store_true", default=False,
        help="Modo interactivo")
    parser.add_option("-c", "--consolepwd", dest="consolepwd",
        help="Clave para el modo interactivo remoto (puerto normal + 1). Si no se pone, no habra modo interactivo")
    (opciones, args) = parser.parse_args()
    if opciones.port<1024 or opciones.port>65000:
    	print("Bad port")
    	parser.print_help()
    	sys.exit(-1)
    gestor=CGestorColas(opciones.sfichDB)
    if opciones.deleteQueue!=None:
        for opt in opciones.deleteQueue:
            if "ALL"==opt:
                gestor.emptyAllQueues()
            else:
                try:
                    idQueue=int(opt)
                    gestor.emptyQueue(idQueue)
                except:
                    print("Error en el id de la cola del servidor {0}".format(opt))
                    sys.exit(-1)


    delayManager=DelayManager(gestor)
    ReceptorColaMensajes.gestorColas=gestor
    # Create the server, binding to localhost on port 9999
    #Servidor mono-thread
    #server = SocketServer.TCPServer((HOST, PORT), ReceptorColaMensajes)
    #Servidor multithread
    server = ThreadedTCPServer((opciones.host, opciones.port), ReceptorColaMensajes)


    # Start a thread with the server -- that thread will then start one
    # more thread for each request
    server_thread = threading.Thread(target=server.serve_forever)
    # Exit the server thread when the main thread terminates
    server_thread.daemon = True
    server_thread.start()

    serverConsole=None
    if opciones.consolepwd!=None:
        ReceptorConsola.gestorColas=gestor
        ReceptorConsola.password=opciones.consolepwd
        ReceptorConsola.serverColas=server
        serverConsole=ThreadedTCPServer((opciones.host, opciones.port+1), ReceptorConsola)
        ReceptorConsola.serverConsola=serverConsole
        serverConsole_thread = threading.Thread(target=serverConsole.serve_forever)
        serverConsole_thread.daemon = True
        serverConsole_thread.start()

    # Activate the server; this will keep running until you
    # interrupt the program with Ctrl-C
    # server.serve_forever()
    print("Escuchando cola en servidor {1} puerto {0}".format(opciones.port, opciones.host))
    if not opciones.interactive:
        print("Pulsa q para salir")
        pulsado = False
        while server_thread.isAlive() and not pulsado:
            server_thread.join(1.0)
            tecla = getch.getchNoBloqueante()
            if tecla == 'q':
                pulsado = True
    else:
        readline.set_completer(completer)
        readline.parse_and_bind("tab: complete")
        finBucle = False
        while not finBucle:
            entrada = raw_input("> ")
            entrada = entrada.strip()
            salida = interpretaEntrada(gestor, entrada)
            if isinstance(salida, basestring):
                print(salida)
            else:
                finBucle = salida
        delayManager.indicaTocaTerminar(True)
