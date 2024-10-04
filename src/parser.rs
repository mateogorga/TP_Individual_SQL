use crate::errors;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::fs::File;

/// Recibe la consulta ingresada por el usuario y devuelve un vector de strings. 
/// Dentro de los parentesis agrupa cadenas y
/// fuera de ellos, separa por comas y espacios.
pub fn dividir_consulta(consulta: &str) -> Vec<String> { 
    let mut nueva_lista: Vec<String> = Vec::new(); 
    let mut acumulador = String::new();
    let mut en_parentesis = false;

    for c in consulta.chars() {
        match c {
            '(' => {
                if !acumulador.is_empty() {
                    nueva_lista.push(acumulador.trim().to_string()); 
                    acumulador.clear();
                }
                en_parentesis = true;
                acumulador.push(c);
            }
            ')' => {
                acumulador.push(c);
                nueva_lista.push(acumulador.trim().to_string()); 
                acumulador.clear();
                en_parentesis = false;
            }
            ',' => {
                if en_parentesis {
                    acumulador.push(c);
                } else {
                    if !acumulador.is_empty() {
                        nueva_lista.push(acumulador.trim().to_string()); 
                        acumulador.clear();
                    } else {
                        acumulador.push(c);
                    }
                }
            }
            ' ' => {
                if !en_parentesis && !acumulador.is_empty() {
                    nueva_lista.push(acumulador.trim().to_string()); 
                    acumulador.clear();
                } else {
                    acumulador.push(c);
                }
            }
            ';' => {
                if !acumulador.is_empty() {
                    nueva_lista.push(acumulador.trim().to_string()); 
                }
            }
            _ => acumulador.push(c),
        }
    }
    nueva_lista
}


///Recibe la ruta donde estan los archivos y el nombre de la tabla.
///Devuelve la ruta completa a la tabla.
pub fn generar_ruta(ruta: &str, nombre_tabla: &str) -> String{
    ruta.to_string() + "/" + nombre_tabla + ".csv"
}


///Convierte un String a i32.
pub fn convertir_a_entero(num: &String) -> i32{
    let entero: i32 = match num.parse::<i32>() {
        Ok(valor) => valor,
        Err(_) => {
            errors::mostrar_error(errors::ErrorType::Error, "Error al convertir el entero");            0 // Valor predeterminado
        },
    };

    entero
}


///Recibe un BufReader del archivo.
///Devuelve un String con el nombre de las columnas.
pub fn obtener_nombres_columnas(reader: &mut BufReader<File>) -> String {
    let mut lineas = reader.lines();

    let primera_linea = match lineas.next() {
        Some(Ok(line)) => line,
        Some(Err(_e)) => {
            errors::mostrar_error(errors::ErrorType::InvalidColumn, "Error al leer el nombre de las columnas");
            return ' '.to_string();
        },
        None => {errors::mostrar_error(errors::ErrorType::InvalidTable, "El archivo esta vacio.");
                return ' '.to_string();},
    };

    primera_linea
}


#[cfg(test)]
mod tests {
    use super::*;

    // Prueba para la función dividir_consulta
    #[test]
    fn test_dividir_consulta() {
        let consulta = "INSERT INTO clientes (id, nombre, apellido) VALUES (20, 'Lucas', 'Perez'), (21, 'Juan', 'Benitez'), (22, 'Maria', 'Fernandez');".to_string();
        let resultado = dividir_consulta(&consulta);
        assert_eq!(resultado, vec!["INSERT", "INTO", "clientes", "(id, nombre, apellido)", "VALUES", "(20, 'Lucas', 'Perez')", ",", "(21, 'Juan', 'Benitez')", ",", "(22, 'Maria', 'Fernandez')"]);
    }
}