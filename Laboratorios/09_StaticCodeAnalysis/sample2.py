# Define una función llamada 'add' que toma dos argumentos, 'number1' y 'number2'.
# El propósito de esta función es sumar los dos números y devolver el resultado.
def add(number1, number2):
    # Devuelve la suma de 'number1' y 'number2'.
    # Esta línea calcula la adición de los dos números de entrada y devuelve el resultado.
    return number1 + number2

# Inicializa la variable constante 'NUM1' con el valor 4.
# Las constantes suelen escribirse en letras mayúsculas para indicar que no deben cambiarse.
NUM1 = 4

# Inicializa la variable 'num2' con el valor 5.
# Esta variable se utilizará como la segunda entrada para la función 'add'.
num2 = 5

# Llama a la función 'add' con 'NUM1' y 'num2' como argumentos.
# El resultado de esta operación de adición se almacena en la variable 'total'.
total = add(NUM1, num2)

# Imprime una cadena formateada que muestra la suma de 'NUM1' y 'num2'.
# El método 'format' se utiliza para insertar los valores de 'NUM1', 'num2' y 'total' en la cadena.
print("La suma de {} y {} es {}".format(NUM1, num2, total))