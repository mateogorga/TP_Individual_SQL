use std::env;
use std::fmt::Error;
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::fs::OpenOptions;
use std::fs;

mod errors;
mod parser;
mod insert;


fn select_where(consulta: Vec<String>, ruta: &str) {
    let componentes = consulta.len();

    //Abro el archivo que contiene a la tabla
    let ruta_completa = parser::generar_ruta(ruta, consulta[3].as_str());

    let f = match File::open(ruta_completa) {
        Ok(f) => f,
        Err(e) => {
            errors::mostrar_error(errors::ErrorType::InvalidTable, "Error al leer la tabla");
            return;
        },
    };

    //Creo un buffer para leer el archivo 
    let mut reader = BufReader::new(f);

    //Obtengo el nombre de las columnas
    let nombres_columnas: String = parser::obtener_nombres_columnas(&mut reader);
    let columnas: Vec<&str> = nombres_columnas.trim_matches(|c| c == '(' || c == ')').split(",").map(|s| s.trim()).collect(); 

    //Creo la salida de la funcion
    let mut salida = io::stdout();

    for linea in reader.lines() {
        match linea {
            Ok(line) => {
                if componentes == 8 {
                    if consulta[6] == ">" {
                        let mut i = 0;
                        while i <= columnas.len()-1 {
                            if columnas[i] == consulta[5] {
                                let lista_linea: Vec<&str> = line.trim_matches(|c| c == '(' || c == ')').split(",").map(|s| s.trim()).collect(); 
                                let entero_consulta: i32 = parser::convertir_a_entero(&consulta[7]);
                                let entero_linea: i32 = parser::convertir_a_entero(&lista_linea[i].to_string());
    
                                if entero_linea > entero_consulta {
                                    if writeln!(salida, "{}", line).is_err() {
                                        errors::mostrar_error(errors::ErrorType::Error, "Error al escribir el archivo");
                                        return;
                                    }
                                }                              
                            } 
                            i+=1;
                        }
                        
                    } else if consulta[6] == "<" {
                        let mut i = 0;
                        while i <= columnas.len()-1 {
                            if columnas[i] == consulta[5] {
                                let lista_linea: Vec<&str> = line.trim_matches(|c| c == '(' || c == ')').split(",").map(|s| s.trim()).collect(); 
                                let entero_consulta: i32 = parser::convertir_a_entero(&consulta[7]);
                                let entero_linea: i32 = parser::convertir_a_entero(&lista_linea[i].to_string());
    
                                if entero_linea < entero_consulta {
                                    if writeln!(salida, "{}", line).is_err() {
                                        errors::mostrar_error(errors::ErrorType::Error, "Error al escribir el archivo");
                                        return;
                                    }
                                }                              
                            }
                            i+=1;
                        }
                    } else if consulta[6] == "=" {
                        let mut i = 0;
                        while i <= columnas.len()-1 {
                            if columnas[i] == consulta[5] {
                                let lista_linea: Vec<&str> = line.trim_matches(|c| c == '(' || c == ')').split(",").map(|s| s.trim()).collect(); 
                                let entero_consulta: i32 = parser::convertir_a_entero(&consulta[7]);
                                let entero_linea: i32 = parser::convertir_a_entero(&lista_linea[i].to_string());
    
                                if entero_linea == entero_consulta {
                                    if writeln!(salida, "{}", line).is_err() {
                                        errors::mostrar_error(errors::ErrorType::Error, "Error al escribir el archivo");
                                        return;
                                    }
                                }                              
                            }
                            i+=1;
                        }
                    }
                } else {

                }      
            }
            Err(e) => {
                errors::mostrar_error(errors::ErrorType::InvalidTable, "Error al leer la linea");
                return;
            }
        }
    }
}

fn select_order_by() {
    //FALTA IMPLEMENTAR
}

fn select(consulta: Vec<String>, ruta: &str) {
    let comp_where = "WHERE".to_string();
    let comp_order_by = "ORDER".to_string();

    match &consulta[4] {
        comp_where => select_where(consulta, ruta),
        comp_order_by => select_order_by(),
        _ => errors::mostrar_error(errors::ErrorType::InvalidSintax, "Error al procesar la consulta"),
    }
}


fn update(consulta: Vec<String>, ruta: &str) {
    // Abro el archivo que contiene a la tabla
    let ruta_completa = parser::generar_ruta(ruta, &consulta[1]);

    let f = match File::open(&ruta_completa) {
        Ok(f) => f,
        Err(e) => {
            errors::mostrar_error(errors::ErrorType::InvalidTable, &format!("Error al leer la tabla: {}", e));
            return;
        },
    };

    // Creo un buffer para leer el archivo 
    let mut reader = BufReader::new(f);

    // Obtengo el nombre de las columnas
    let nombres_columnas: String = parser::obtener_nombres_columnas(&mut reader);

    // Creo un archivo temporal donde agregaré las nuevas filas
    let path_new = PathBuf::from("archivo_temporal.csv");
    let file_new = match OpenOptions::new().write(true).create(true).truncate(true).open(&path_new) {
        Ok(f) => f,
        Err(e) => {
            errors::mostrar_error(errors::ErrorType::Error, "Error al crear el archivo temporal.");
            return;
        },
    };

    // Agrego las líneas que ya estaban
    let mut writer = BufWriter::new(file_new);

    // Agrego la primera línea
    if writeln!(writer, "{}", nombres_columnas).is_err() {
        errors::mostrar_error(errors::ErrorType::Error, "Error al escribir el archivo");
        return;
    }

    // Determino la posición de la columna a actualizar
    let long = consulta.len();
    let columnas_totales: Vec<&str> = nombres_columnas.trim_matches(|c| c == '(' || c == ')').split(',').map(|s| s.trim()).collect();

    let num_cols = columnas_totales.len();
    let mut posicion = 0;

    for i in 0..columnas_totales.len() {
        if columnas_totales[i] == consulta[long - 3] {
            posicion = i;
            break;
        }
    }

    // Itero sobre las líneas del archivo original
    for linea in reader.lines() {
        match linea {
            Ok(l) => {
                let elementos: Vec<&str> = l.split_whitespace().collect();
                let consulta_valor_indice = &consulta[long - 1];
                let consulta_col_cambiar = &consulta[3];

                if elementos.get(posicion) == Some(&consulta_valor_indice.as_str()) {
                    let mut nueva_linea: Vec<&str> = Vec::with_capacity(num_cols);

                    for i in 0..num_cols {
                        if columnas_totales[i] == consulta_col_cambiar {
                            nueva_linea.push(&consulta[5]);
                        } else {
                            if let Some(elem) = elementos.get(i) {
                                nueva_linea.push(elem);
                            }
                        }
                    }

                    let nueva_linea_str = nueva_linea.join(" ");
                    if writeln!(writer, "{}", nueva_linea_str).is_err() {
                        errors::mostrar_error(errors::ErrorType::Error, "Error al escribir el archivo");
                        return;
                    }
                } else {
                    if writeln!(writer, "{}", l).is_err() {
                        errors::mostrar_error(errors::ErrorType::Error, "Error al escribir el archivo");
                        return;
                    }
                }
            }
            Err(e) => {
                errors::mostrar_error(errors::ErrorType::Error, "Error al leer el archivo");
                return;
            }
        }
    }

    //Cambio el nombre del archivo temporal
    let nombre_csv = consulta[1].to_string() + ".csv";
    let _ = fs::rename("archivo_temporal.csv", nombre_csv);
}

fn delete() {
    //FALTA IMPLEMENTAR
}



fn main() {

    //Obtengo un vector con los ingresos
    let ingresos : Vec<String> = env::args().collect();
    let ruta_tablas = &ingresos[1];
    let lista_consulta = parser::dividir_consulta(&ingresos[2]); 

    match lista_consulta[0].to_uppercase().as_str() {
        "" => errors::mostrar_error(errors::ErrorType::InvalidSintax, "La consulta esta vacia"),
        "INSERT" => insert::insert_into(lista_consulta, ruta_tablas),
        "SELECT" => select(lista_consulta, ruta_tablas),
        "UPDATE" => update(lista_consulta, ruta_tablas),
        "DELETE" => delete(),
        _ => errors::mostrar_error(errors::ErrorType::InvalidSintax, "Error en el ingreso de la consulta."),
    }
}

