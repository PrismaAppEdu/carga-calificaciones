/**
 * ================================================================
 * SyncServiceOrchestrator.gs — ORQUESTA: Lee + Transforma + Guarda
 * ================================================================
 */

// Crear instancias de los módulos
var reader = null;
var persistence = null;

function _inicializarModulos() {
  if (!reader) {
    // reader = new DataReaderRTDB();  // Si lo conviertes a clase
  }
  if (!persistence) {
    // persistence = new DataPersistenceRTDB();
  }
}

/**
 * FUNCIÓN PRINCIPAL: Llamada desde WebApp.html
 * Lee datos del RTDB fuente, procesa, y guarda en RTDB destino
 */
function sincronizarCalificacionesCompleta(datosImportacion) {
  var tiempoInicio = new Date();
  var lock = LockService.getScriptLock();
  
  try {
    lock.waitLock(10000);
  } catch (e) {
    return {
      ok: false,
      error: "Servidor ocupado. Intenta en unos segundos."
    };
  }
  
  try {
    var profesor = datosImportacion.profesor.trim();
    var matricula = datosImportacion.matricula.trim();
    var tipoGrupo = datosImportacion.tipoGrupo;
    
    // 1. LEER DATOS DEL PROFESOR DESDE RTDB FUENTE
    var datosProfesor = _leerProfesorRTDB(matricula);
    if (!datosProfesor.ok) {
      return datosProfesor;
    }
    
    // 2. PROCESAR GRUPOS Y CALIFICACIONES
    var filasParaAgregar = [];
    var resumen = [];
    var gruposLog = [];
    var asignaturasLog = [];
    
    datosImportacion.gruposLinks.forEach(function(item) {
      var grupo = item.grupo.trim();
      var asignatura = item.asignatura.trim();
      var link = item.link.trim();
      
      if (!grupo || !asignatura || !link) return;
      
      try {
        // Validar link
        var validacion = validarLink(link);
        if (!validacion.ok) {
          resumen.push("❌ ERROR: Link inválido en " + grupo);
          return;
        }
        
        // Leer desde Classroom
        var datosOrigen = SpreadsheetApp.openById(validacion.id)
          .getSheets()[0].getDataRange().getValues();
        
        if (datosOrigen.length < 5) {
          resumen.push("⚠ OMITIDO: " + grupo + " — muy pocas filas");
          return;
        }
        
        // Procesar calificaciones
        var registros = _procesarClassroom(datosOrigen, profesor, grupo, asignatura);
        filasParaAgregar = filasParaAgregar.concat(registros);
        
        gruposLog.push(grupo);
        if (asignaturasLog.indexOf(asignatura) === -1) {
          asignaturasLog.push(asignatura);
        }
        
        resumen.push("✅ " + grupo + " — " + registros.length + " registro(s)");
        
      } catch (err) {
        resumen.push("❌ Error en \"" + grupo + "\": " + err.message);
      }
    });
    
    // 3. GUARDAR EN RTDB DESTINO (estructura jerárquica)
    if (filasParaAgregar.length > 0) {
      _guardarCalificacionesRTDB(filasParaAgregar, matricula, profesor);
      _registrarBitacoraRTDB(profesor, matricula, gruposLog, asignaturasLog, filasParaAgregar.length);
      _actualizar DashboardRTDB(profesor, matricula, gruposLog);
    }
    
    // 4. TAMBIÉN GUARDAR EN SHEETS (para compatibilidad)
    if (filasParaAgregar.length > 0) {
      _guardarEnSheets(filasParaAgregar);
      _enviarAFirebase(filasParaAgregar);
    }
    
    var duracion = (new Date().getTime() - tiempoInicio.getTime()) / 1000;
    
    return {
      ok: true,
      mensajeFinal: "¡Listo! Se importaron " + filasParaAgregar.length + " registros.",
      resumen: resumen.join("\n"),
      duracion: duracion
    };
    
  } catch (e) {
    return {
      ok: false,
      error: "Error crítico: " + e.message
    };
  } finally {
    lock.releaseLock();
  }
}

/**
 * Lee profesor desde RTDB fuente
 */
function _leerProfesorRTDB(matricula) {
  try {
    var config = _getFirebaseConfig();
    var url = config.url + "profesores/" + matricula + ".json?auth=" + config.secret;
    
    var resp = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
    if (resp.getResponseCode() !== 200) {
      return { ok: false, error: "Profesor no encontrado en RTDB" };
    }
    
    return { ok: true, data: JSON.parse(resp.getContentText()) };
  } catch (e) {
    return { ok: false, error: "Error leyendo profesor: " + e.toString() };
  }
}

/**
 * Procesa una hoja de Classroom
 */
function _procesarClassroom(datosOrigen, profesor, grupo, asignatura) {
  var registros = [];
  var fechas = datosOrigen[0];
  var actividades = datosOrigen[1];
  
  // Detectar escala
  var escala = 10;
  for (var p = 1; p < datosOrigen[2].length; p++) {
    var maxVal = parseFloat(datosOrigen[2][p]);
    if (!isNaN(maxVal) && maxVal > 0) {
      escala = maxVal;
      break;
    }
  }
  
  // Procesar alumnos
  for (var i = 3; i < datosOrigen.length; i++) {
    var fila = datosOrigen[i];
    var filaString = fila.join(" ").toLowerCase();
    
    if (filaString.includes("media de la clase") || filaString.trim() === "") continue;
    
    // Buscar email
    var idxEmail = -1;
    fila.some(function(celda, idx) {
      if (String(celda).includes("@ibime.edu.mx")) {
        idxEmail = idx;
        return true;
      }
    });
    
    if (idxEmail === -1) continue;
    
    var correo = String(fila[idxEmail]).trim();
    var nombre = fila.slice(0, idxEmail)
      .filter(function(c) { return c && String(c).trim() !== ""; })
      .join(" ").trim() || "Alumno sin nombre";
    
    // Procesar calificaciones
    for (var c = idxEmail + 1; c < fila.length; c++) {
      var hF = fechas[c] ? String(fechas[c]).trim() : "";
      var hA = actividades[c] ? String(actividades[c]).trim() : "";
      if (!hF && !hA) continue;
      
      var rawCalif = fila[c];
      var calif = (rawCalif === "" || rawCalif == null) ? 0 : parseFloat(rawCalif);
      if (escala === 100 && calif > 0) calif = parseFloat((calif / 10).toFixed(1));
      if (calif > 10) calif = 10;
      
      registros.push({
        profesor: profesor,
        grupo: grupo,
        nombre: nombre,
        correo: correo,
        fecha_act: hF,
        actividad: hA,
        calif: calif,
        asignatura: asignatura,
        sync: new Date().toISOString()
      });
    }
  }
  
  return registros;
}

/**
 * Guarda en RTDB (estructura jerárquica)
 */
function _guardarCalificacionesRTDB(datos, matricula, profesor) {
  var config = _getFirebaseConfig();
  if (!config.url || !config.secret) return;
  
  var datoPorProfesor = {};
  
  datos.forEach(function(registro) {
    var grupoKey = registro.grupo.toUpperCase();
    var materiaKey = obtenerAcronimo(registro.asignatura).toUpperCase();
    var correoKey = _sanitizarClave(registro.correo);
    
    if (!datoPorProfesor[grupoKey]) datoPorProfesor[grupoKey] = {};
    if (!datoPorProfesor[grupoKey][materiaKey]) datoPorProfesor[grupoKey][materiaKey] = {};
    if (!datoPorProfesor[grupoKey][materiaKey][correoKey]) {
      datoPorProfesor[grupoKey][materiaKey][correoKey] = {
        nombre: registro.nombre,
        correo: registro.correo,
        actividades: []
      };
    }
    
    datoPorProfesor[grupoKey][materiaKey][correoKey].actividades.push({
      fecha: registro.fecha_act,
      actividad: registro.actividad,
      calif: registro.calif,
      sync: registro.sync
    });
  });
  
  var payloadProfesor = {};
  payloadProfesor[matricula] = datoPorProfesor;
  
  _escribirEnRTDB("/calificaciones/por_profesor", payloadProfesor);
}

/**
 * Registra bitácora en RTDB
 */
function _registrarBitacoraRTDB(prof, matricula, grupsLog, matsLog, cantidad) {
  var config = _getFirebaseConfig();
  if (!config.url || !config.secret) return;
  
  var hoy = new Date().toISOString().split('T')[0];
  var bitacoraId = "bitacora_" + new Date().getTime();
  
  var bitacora = {
    timestamp: new Date().toISOString(),
    profesor: prof,
    matricula: matricula,
    grupos: grupsLog,
    materias: matsLog,
    registrosAgregados: cantidad,
    estado: "exitoso"
  };
  
  _escribirEnRTDB("/bitacora/" + hoy + "/" + bitacoraId, bitacora, "PUT");
}

/**
 * Actualiza dashboard en RTDB
 */
function _actualizarDashboardRTDB(profesor, matricula, gruposLog) {
  var config = _getFirebaseConfig();
  if (!config.url || !config.secret) return;
  
  var hoy = new Date().toISOString().split('T')[0];
  var dashboardData = {
    profesor: profesor,
    matricula: matricula,
    estado: "✅ COMPLETADO",
    gruposCargados: gruposLog.length,
    ultimaCarga: new Date().toISOString()
  };
  
  _escribirEnRTDB("/dashboards/maestro_resumen/" + hoy + "/" + matricula, dashboardData, "PUT");
}

/**
 * Helper: Escribir en RTDB
 */
function _escribirEnRTDB(path, data, method) {
  method = method || "PUT";
  var config = _getFirebaseConfig();
  
  var url = config.url + path + ".json?auth=" + config.secret;
  
  try {
    var resp = UrlFetchApp.fetch(url, {
      method: method,
      contentType: "application/json",
      payload: JSON.stringify(data),
      muteHttpExceptions: true
    });
    
    if (resp.getResponseCode() !== 200) {
      console.error("Error en RTDB: HTTP " + resp.getResponseCode());
    }
  } catch (e) {
    console.error("Error: " + e.toString());
  }
}

/**
 * Helper: Guardar en Sheets (para compatibilidad)
 */
function _guardarEnSheets(filasParaAgregar) {
  try {
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    var hojaDestino = ss.getSheetByName(HOJAS.MAESTRO);
    
    if (!hojaDestino) {
      hojaDestino = ss.insertSheet(HOJAS.MAESTRO);
      hojaDestino.appendRow(["Profesor", "Grupo", "Alumno", "Correo", "Fecha", "Actividad", "Calificación", "Asignatura"]);
    }
    
    hojaDestino.getRange(
      hojaDestino.getLastRow() + 1, 1,
      filasParaAgregar.length, 8
    ).setValues(filasParaAgregar.map(function(f) {
      return [f.profesor, f.grupo, f.nombre, f.correo, f.fecha_act, f.actividad, f.calif, f.asignatura];
    }));
  } catch (e) {
    console.error("Error guardando en Sheets: " + e);
  }
}
