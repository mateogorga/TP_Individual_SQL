use crate::parser;
use crate::errors;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::fs::OpenOptions;
use std::fs;


fn crear_archivo() -> Result<File, ()> {
    let path_new = PathBuf::from("archivo_temporal.csv");
    let file_new = match OpenOptions::new().write(true).create(true).truncate(true).open(&path_new) {
        Ok(f) => f,
        Err(e) => {
            errors::mostrar_error(errors::ErrorType::Error, "Error al crear el archivo temporal.");
            return Err(());
        },
    };
    Ok(file_new)
}


fn agregar_lineas(writer: &mut BufWriter<File>, nombres_columnas: &String, reader: &mut BufReader<File>) {
    let lineas = reader.lines();

    if writeln!(writer, "{}", nombres_columnas).is_err() {
        errors::mostrar_error(errors::ErrorType::Error, "Error al escribir el archivo");
        return;
    }

    for linea in lineas {
        match linea {
            Ok(l) => {
                if writeln!(writer, "{}", l).is_err() {
                    errors::mostrar_error(errors::ErrorType::Error, "Error al escribir el archivo");
                    return;
                }
            }
            Err(e) => {
                errors::mostrar_error(errors::ErrorType::Error, "Error al leer el archivo");
                return;
            }
        }
    }
}


pub fn insert_into(consulta: Vec<String>, ruta: &str) {
    let mut cont_filas = 5;
    //Abro el archivo que contiene a la tabla
    let ruta_completa = parser::generar_ruta(ruta, consulta[2].as_str());

    let f = match File::open(ruta_completa) {
        Ok(f) => f,
        Err(e) => {
            errors::mostrar_error(errors::ErrorType::InvalidTable, &format!("Error al leer la tabla: {}", e));
            return;
        },
    };

    //Creo un buffer para leer el archivo 
    let mut reader = BufReader::new(f);

    //Obtengo el nombre de las columnas
    let nombres_columnas: String = parser::obtener_nombres_columnas(&mut reader);

    //Creo un archivo temporal donde agregaré las nuevas filas
    let file_new: Result<File, ()> = crear_archivo();

    //Agrego las lineas que ya estaban
    let file = match file_new {
        Ok(f) => f,
        Err(_) => return (),
    }; 
    let mut writer = BufWriter::new(file);
    agregar_lineas(&mut writer, &nombres_columnas, &mut reader);

    //Agrego los nuevos valores
    let columnas_nuevas: Vec<&str> = consulta[3].trim_matches(|c| c == '(' || c == ')').split(",").map(|s| s.trim()).collect();
    let columnas_totales: Vec<&str> = nombres_columnas.trim_matches(|c| c == '(' || c == ')').split(",").map(|s| s.trim()).collect(); 

    while cont_filas < consulta.len() {
        let consulta_fila = consulta[cont_filas].trim_matches(|c| c == '(' || c == ')').replace("'", "");
        let mut contenido = consulta_fila.split(',').map(|s| s.trim()).collect::<Vec<&str>>();
        let mut i = 0;

        for col in &columnas_totales {
            if !columnas_nuevas.contains(&col) {
                contenido.insert(i, "null");
            }
            i+=1;
        }

        //Agrego la nueva fila al archivo temporal
        if writeln!(writer, "{}", contenido.join(",")).is_err() {
            errors::mostrar_error(errors::ErrorType::Error, "Error al escribir el archivo");
            return;
        }

        cont_filas+=2;
    }

    //Cambio el nombre del archivo temporal
    let nombre_csv = consulta[2].to_string() + ".csv";
    let _ = fs::rename("archivo_temporal.csv", nombre_csv);

}