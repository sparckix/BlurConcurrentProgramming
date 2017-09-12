'''Archivo de ayuda para el procesamiento de la imagen. Hay que tener en cuenta que se trata de una simulacion y que por tanto
se usa memoria compartida. En un caso real distribuido la imagen podria estar en un servidor central y cada nodo cogeria su "trozo", por ejemplo'''
import sys
import Image, numpy, ImageFilter
if len(sys.argv) < 2 or len(sys.argv) > 2:
	sys.exit('ERROR. Uso: python dsFiltrado.py images/imagen')

try:
	img = Image.open(sys.argv[1])
except:
	sys.exit("ERROR. No se ha podido abrir la imagen")
imgArr = numpy.asarray(img) # readonly

#Ancho y alto en pixeles.
imgWidth = imgArr.shape[1]
imgHeight = imgArr.shape[0]


'''SE DEJA COMENTADO. NO RESULTO VIABLE.
# blur radius in pixels
radius = 2

# blur window length in pixels
windowLen = radius*2+1

#simple box/window blur
def doblur(imgArr, width, height, k, l):
    # create array for processed image based on input image dimensions
    k = l
    imgB = numpy.zeros((imgHeight,width,3),numpy.uint8)
    imgC = numpy.zeros((imgHeight,width,3),numpy.uint8)
    # blur horizontal row by row
    for ro in range(1,height):
        # RGB color values
        totalR = 0
        totalG = 0
        totalB = 0

        # calculate blurred value of first pixel in each row
        for rads in range(-radius, radius+1):
            if (rads) >= 0 and (rads) <= width-1:
                totalR += imgArr[ro,rads][0]/windowLen
                totalG += imgArr[ro,rads][1]/windowLen
                totalB += imgArr[ro,rads][2]/windowLen

        imgB[ro,l] = [totalR,totalG,totalB]

        # calculate blurred value of the rest of the row based on
        # unweighted average of surrounding pixels within blur radius
        # using sliding window totals (add incoming, subtract outgoing pixels)
        for co in range(l,width):
            if (co-radius-1) >= 0:
                totalR -= imgArr[ro,co-radius-1][0]/windowLen
                totalG -= imgArr[ro,co-radius-1][1]/windowLen
                totalB -= imgArr[ro,co-radius-1][2]/windowLen
            if (co+radius) <= width-1:
                totalR += imgArr[ro,co+radius][0]/windowLen
                totalG += imgArr[ro,co+radius][1]/windowLen
                totalB += imgArr[ro,co+radius][2]/windowLen

            # put average color value into imgB pixel

            imgB[ro,co] = [totalR,totalG,totalB]

    # blur vertical

    for co in range(l,width):
        totalR = 0
        totalG = 0
        totalB = 0

        for rads in range(-radius, radius+1):
            if (rads) >= 0 and (rads) <= height-1:
                totalR += imgB[rads,co][0]/windowLen
                totalG += imgB[rads,co][1]/windowLen
                totalB += imgB[rads,co][2]/windowLen

        imgC[0,co] = [totalR,totalG,totalB]

        for ro in range(1,height):
            if (ro-radius-1) >= 0:
                totalR -= imgB[ro-radius-1,co][0]/windowLen
                totalG -= imgB[ro-radius-1,co][1]/windowLen
                totalB -= imgB[ro-radius-1,co][2]/windowLen
            if (ro+radius) <= height-1:
                totalR += imgB[ro+radius,co][0]/windowLen
                totalG += imgB[ro+radius,co][1]/windowLen
                totalB += imgB[ro+radius,co][2]/windowLen

            imgC[ro,co] = [totalR,totalG,totalB]

    return imgB

# number of times to run blur operation
blurPasses = 1

# temporary image array for multiple passes
imgTmp = imgArr'''
