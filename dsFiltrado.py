""" Implementado mediante polling de mensajes en lugar de estructura de procesos separada 
    dentro de un mismo nodo.
    El nodo entorno es el encargado de crear los procesos.
    Se difumina por tiras a lo ancho (toda la altura) como mucho 50 pixeles (caso trivial, ver mas abajo)
    
    Utilizacion: python dsFiltrado.py images/prueba.jpg
    Autores: Daniel Alami y Maria Rodriguez
    
    DON'T DO HARM ;).

    Dependencias (pip): portalocker, numpy, beanstalkc, pydot, pyyaml
    Tambien se requiere tener instalado GraphViz
 """

import sys, os, multiprocessing, subprocess, time, random
#Tratamiento de imagenes
import Image, numpy, ImageFilter
import beanstalkc
#Expresiones regulares "de andar por casa" para procesar red_nodos
import re
#Exclusion mutua a nivel de fichero estilo FLOCK. Cedido por Jonathan Feinberg
import portalocker
#Libreria para procesar y generar imagenes a partir de DOT
import pydot
#Utilidades internas
import blur

#Clase para enviar los mensajes totales. Homogenea para comprobacion de tipos segura en el nodo entorno
#y por si se quiere expandir en un futuro.
class MessageInfo:
    def __init__(self, sended):
	self.sended = sended

    def getSended(self):
        return self.sended

#Clase que almacena el resultado de un trabajo para enviar a la cola del nodo entorno (contiene la imagen y las posiciones relativas)
class ResultImageClass:
    def __init__(self, imgen, x, y, w, h):
        self.imgen, self.x, self.y, self.w, self.h = imgen, x, y, w, h

    def getBox(self):
	return (self.x, self.y, self.w, self.h)

    def getI(self):    
	return self.imgen

#Funcion auxiliar para saber con quien se puede comunicar un nodo. Con esta funcion
#nos aseguramos que ningun nodo envia solo a quien puede segun las conexiones del grafo.
#Lee de red_nodos; no lo hemos hecho dirigido sino que se han hecho explicitas las conexiones para hacerlo mas facil
def communicate(i):
	canSend = []
    	with open('red_nodos') as f:
	 for line in f:
                match = re.search('^' + str(i) + '..*',line)
                if match:
                        words = match.group().split()
                        if words[0] == str(i):
				canSend.append(words[2])
	return canSend;

def children(q, i, tuberia_propia):
    '''Iteraciones maximas a las que se llega para considerar que no se recibiran mas trabajos (job done).
    Cuando llega a este maximo es cuando se notifica a los que le han enviado trabajos y al final al padre.'''
    threshold = 7
    iterations = trivial = working = sended = 0
    '''Condicion que imponemos nosotros al procesado de imagenes:
    para un trabajo X (trozo de imagen) un nodo Y solo puede partirlo en 2 
    (y enviarselo a otros 2 respectivamente).'''
    msgEnv = 2
    canSend = []
    '''Variables propias del algoritmo CS. El array de indeficit 
    podria haberse definido de otra manera, pero sabemos que son 15 nodos y por simplificacion
    lo hemos "embebido" en la logica del programa. 
    Otra opcion podria haber sido pasar por parametro el numero total de nodos, o sacarlo a partir
    del archivo dot'''
    ainDeficit = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    inDeficit = outDeficit = 0
    parent = -1

    time.sleep(4) #sys cooldown
    beanstalk_children = beanstalkc.Connection(host='127.0.0.1', port=11300) 
    beanstalk_children.watch(tuberia_propia) 
    #Leer el archivo y ver con quien se puede comunicar                               
    canSend = communicate(i)
    while True:
	trivial = 0
	job = beanstalk_children.reserve(2)
	img_result = None 
        
	#Seccion que se ejecuta cuando ha acabado el trabajo y no hemos recibido ninguno mas.
	#Replica del algoritmo CS enviar signal.
        if working == 0 and iterations > threshold:
                if inDeficit > 1:
			found = 0
			for E in range(len(ainDeficit)):
		   	   if (ainDeficit[E] > 1) or (ainDeficit[E] == 1 and E != int(parent)):
			       found = E
			       break

                        beanstalk_children.use(str(found))
                        beanstalk_children.put('SGN,' + str(i), 100)

			sended += 1
			ainDeficit[found] -= 1
			inDeficit -= 1
                elif inDeficit == 1 and outDeficit == 0:
                        beanstalk_children.use(parent);
                        beanstalk_children.put('SGN,' + str(i), 100)
			sended += 1
			ainDeficit[int(parent)] = 0
			inDeficit = 0
			parent = -1
			msgi = MessageInfo(sended)
			q.put(msgi)

        if type(job) == type(None):
		#Caso en que no se ha recibido ningun trabajo. Simplemente pasamos.		
		iterations += 1
                continue

	#Si estamos aqui tenemos un mensaje por procesar. Miramos si es un mensaje o un signal.
	#Si es un mensaje, aqui es donde se hace la computacion del difuminado del trozo de imagen.
        job.delete()
	a = job.body.split(",")
	if a[0] == 'MSG':
		if parent == -1:
			parent = a[1]
			filep = open("resultado.dot", "a")
			#POSIX Flock. Iteramos "un poco" hasta conseguir el fichero, por si acaso
			#esta siendo usado por otro nodo.
		  	for iterador in range(0,10):
            			try:               
					portalocker.lock(filep, portalocker.LOCK_EX | portalocker.LOCK_NB)
					break
				except portalocker.LockException:
					time.sleep(0) #no haria ni falta
			filep.write(parent + "->" + str(i) + ";\n")
			portalocker.unlock(filep)
			filep.close()

                #Nos ha enviado un trabajo, tener en cuenta que no podemos enviarle nada.
		ainDeficit[int(a[1])] += 1
		inDeficit += 1
		iterations = 0
		working = 1

		#Configuracion de variables para el procesado de la imagen
	   	var_y = int(a[3])
                var_x = (int(a[2]) + int(a[4]))/2
                l = int(a[4])
		k = l

		#Lista de nodos a los que puedo enviar un trabajo. Elijo a 2 para enviar.
		#Precondicion: templist >= 2, sino el nodo hace todo el trabajo.
                templist = [elem for elem in canSend if ainDeficit[int(elem)] < 1]
		length = len(templist)
	
		while (msgEnv != 0):
		 if (((int(a[2]) - int(a[5])) > 50) and len(templist) != 0 and len(templist) >= 2):
			#Elegimos uno al azar a quien enviar de los posibles
			pe = templist.pop(random.randrange(len(templist)))
			msgEnv -= 1
			beanstalk_children.use(str(pe));
			beanstalk_children.put('MSG,' + str(i) + ',' + str(var_x)
                                       + ',' + str(var_y) + ',' + str(k) + ',' + str(l), 30)
			outDeficit += 1
			sended += 1
		        var_x = int(a[2])
                     	l = (int(a[2]) + int(a[4]))/2
			k = l
		 else:
		   trivial = 1
		   break

		msgEnv = 2
		#Caso trivial que podemos procesar, no hace falta seguir cortando la imagen
		if trivial:
		    region = blur.img.crop((int(a[5]),0,int(a[2]),blur.imgHeight))
		    region = region.filter(ImageFilter.BLUR)
		    img_result = numpy.asarray(region)
		    #trabajo finalizado

	#Se trata de un signal, no hace falta comprobarlo porque sabemos que es esto
	#(solo enviamos dos tipos de mensajes)
	elif a[0] == 'SGN':
		outDeficit -= 1
	if working == 1 and trivial == 1:
		try:
			result = ResultImageClass(img_result, int(a[5]), 0, int(a[2]), blur.imgHeight)
			q.put(result)
		except: 
		        print "Error inesperado:", sys.exc_info()[0]

	#reset de valores para una futura ronda. No estamos trabajando
	working = 0
        #seguimos dentro del while true

#Nodo entorno
if __name__ == '__main__':

    outDeficit = 0
    sended = 0
    beanstalk = beanstalkc.Connection(host='127.0.0.1', port=11300)
    beanstalk.watch('0') 
    #Iterador por si queremos hacer varias (caso 5 ejecuciones algoritmo basico del enunciado). En realidad se podria quitar (solo se ejecuta 1 vez)
    #y desplazar a la izquierda el codigo indentado, pero se deja por si acaso.
    for it in range(1):
	canSend = []
        outDeficit = 0

        start_time = time.time()
	filep = open("resultado.dot", "a")
	filep.write("digraph spanning_tree {" + '\n')
	filep.close()
    	jobs = []
	q = multiprocessing.Queue()
     	for i in range(14):
        	p = multiprocessing.Process(target=children, args=(q,i+1, str(i+1),))
        	jobs.append(p)
        	p.start()

	#Variables de division de imagen (el nodo entorno parte la imagen en dos y envia los trabajos)
        i = 0
	var_y = blur.imgHeight
	var_x = blur.imgWidth/2
	k = 0 + 1
	l = 0 + 1
 
        """Leer el archivo y ver con quien se puede comunicar. Sabemos que enviamos a 2, por eso no
	anadimos check de seguridad adicional y usamos un simple for, aunque no seria dificil de implementar"""
	canSend = communicate(0)
        for tube in canSend: 
		beanstalk.use(tube)
                beanstalk.put('MSG,' + '0' + ',' + str(var_x) 
			+ ',' + str(var_y) + ',' + str(k) + ',' + str(l), 1)
		sended += 1
		outDeficit += 1
	 	var_x = blur.imgWidth
		l = blur.imgWidth/2
		k = l
                #time.sleep(30) #debug: parentless. Se vera que los que se quedan sin padre tras hacer un trabajo se reenganchan al arbol sin problemas
	while outDeficit != 0:
	        job = beanstalk.reserve()
        	job.delete()
	        a = job.body.split(",")
	        #NO se trata de un mensaje, siempre deberia ser un signal, pero comprobamos por si acaso
        	if a[0] == 'SGN':
			outDeficit -= 1
		else:
			print 'This shouldnt happen'
       
	#Se crea una imagen en blanco y se van pegando los trozos difuminados por los nodos :)~
        blank_image = Image.new(blur.img.mode, blur.img.size, "white")
	print 'Size of queue', q.qsize()
	for inn in range(q.qsize()):
	        result = q.get()
		if isinstance(result, MessageInfo):
			sended += result.getSended()
		else:
			restored = Image.fromarray(numpy.uint8(result.getI()))
			blank_image.paste(restored, result.getBox())
    	for p in jobs:
		p.terminate()
		p.join()

        print 'Termination detected. Sended messages:', sended
        print time.time() - start_time, "seconds"

	blank_image.save('processed-image.jpg', 'JPEG')

        filep = open("resultado.dot", "a")
        filep.write("}")
        filep.close()
	graph = pydot.graph_from_dot_file('resultado.dot')
	graph.write_png('resultado' + str(it) + '.png')
	os.remove('resultado.dot')	
	subprocess.Popen(['gvfs-open', 'processed-image.jpg'])
	subprocess.Popen(['gvfs-open', sys.argv[1]])
	subprocess.Popen(['gvfs-open', 'resultado' + str(it) + '.png'])

