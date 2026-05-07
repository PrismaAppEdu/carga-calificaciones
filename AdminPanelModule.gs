/**
 * ================================================================
 * AdminPanelModule.gs — PANEL DE ADMINISTRACIÓN
 * 
 * PROPÓSITO: Funciones para administradores
 * - Auditoría de cargas
 * - Estadísticas globales
 * - Validaciones de integridad
 * - Limpiezas y respaldos
 * 
 * USO: Admin.getAuditoriaCargas(), getEstadisticasGlobales(), etc.
 * ================================================================
 */

var AdminPanel = {
  
  // ════════════════════════════════════════════════════════════════
  // 1. AUDITORÍA DE CARGAS
  // ════════════════════════════════════════════════════════════════
  
  /**
   * Obtiene historial completo de todas las cargas realizadas
   */
  getAuditoriaCargas: function(diasAtras) {
    diasAtras = diasAtras || 7;
    
    console.log("🔍 Generando auditoría de cargas (últimos " + diasAtras + " días)");
    
    try {
      var config = _getFirebaseConfig();
      var hoy = new Date();
      var url = config.url + "bitacora.json?auth=" + config.secret;
      
      var response = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
      
      if (response.getResponseCode() !== 200) {
        return { ok: false, error: "Sin datos de auditoría" };
      }
      
      var bitacora = JSON.parse(response.getContentText());
      var auditoriaFiltrada = [];
      
      // Filtrar por fechas
      for (var fecha in bitacora) {
        var fechaDate = new Date(fecha);
        var diasDiferencia = Math.floor((hoy - fechaDate) / (1000 * 60 * 60 * 24));
        
        if (diasDiferencia <= diasAtras) {
          for (var id in bitacora[fecha]) {
            auditoriaFiltrada.push(bitacora[fecha][id]);
          }
        }
      }
      
      // Ordenar por fecha descendente
      auditoriaFiltrada.sort(function(a, b) {
        return new Date(b.timestamp) - new Date(a.timestamp);
      });
      
      return {
        ok: true,
        diasFiltro: diasAtras,
        totalCargas: auditoriaFiltrada.length,
        cargas: auditoriaFiltrada
      };
      
    } catch (e) {
      console.error("❌ Error en getAuditoriaCargas: " + e.toString());
      return { ok: false, error: e.toString() };
    }
  },
  
  // ════════════════════════════════════════════════════════════════
  // 2. ESTADÍSTICAS GLOBALES DEL SISTEMA
  // ════════════════════════════════════════════════════════════════
  
  /**
   * Retorna estadísticas de todo el sistema
   * - Total de calificaciones
   * - Total de profesores con carga
   * - Total de alumnos evaluados
   * - Grupos procesados
   */
  getEstadisticasGlobales: function() {
    console.log("📊 Generando estadísticas globales");
    
    try {
      var config = _getFirebaseConfig();
      
      // 1. Contar calificaciones
      var urlCalif = config.url + "calificaciones.json?auth=" + config.secret;
      var responseCalif = UrlFetchApp.fetch(urlCalif, { muteHttpExceptions: true });
      var totalCalificaciones = 0;
      
      if (responseCalif.getResponseCode() === 200) {
        var califs = JSON.parse(responseCalif.getContentText());
        totalCalificaciones = Object.keys(califs || {}).length;
      }
      
      // 2. Contar profesores únicos desde meta_cargas
      var urlMeta = config.url + "meta_cargas.json?auth=" + config.secret;
      var responseMeta = UrlFetchApp.fetch(urlMeta, { muteHttpExceptions: true });
      var profesoresUnicos = new Set();
      var alumnosUnicos = new Set();
      var gruposUnicos = new Set();
      
      if (responseMeta.getResponseCode() === 200) {
        var metas = JSON.parse(responseMeta.getContentText());
        
        for (var fecha in metas) {
          for (var id in metas[fecha]) {
            var meta = metas[fecha][id];
            profesoresUnicos.add(meta.matricula);
            alumnosUnicos.add(meta.totalAlumnos || 0);
            
            if (meta.grupos) {
              meta.grupos.forEach(function(g) {
                gruposUnicos.add(g);
              });
            }
          }
        }
      }
      
      return {
        ok: true,
        fecha_generacion: new Date().toISOString(),
        resumen: {
          totalCalificaciones: totalCalificaciones,
          totalProfesoresConCarga: profesoresUnicos.size,
          totalAlumnosEvaluados: alumnosUnicos.size,
          totalGruposProcessados: gruposUnicos.size
        }
      };
      
    } catch (e) {
      console.error("❌ Error en getEstadisticasGlobales: " + e.toString());
      return { ok: false, error: e.toString() };
    }
  },
  
  // ════════════════════════════════════════════════════════════════
  // 3. VALIDAR INTEGRIDAD DE DATOS
  // ════════════════════════════════════════════════════════════════
  
  /**
   * Valida que los datos en Firebase sean consistentes
   * Busca: calificaciones sin profesor, sin alumno, sin grupo, etc.
   */
  validarIntegridad: function() {
    console.log("🔍 Validando integridad de datos...");
    
    try {
      var config = _getFirebaseConfig();
      var url = config.url + "calificaciones.json?auth=" + config.secret;
      
      var response = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
      
      if (response.getResponseCode() !== 200) {
        return { ok: false, error: "No hay datos para validar" };
      }
      
      var calificaciones = JSON.parse(response.getContentText());
      var errores = [];
      var advertencias = [];
      
      for (var id in calificaciones) {
        var cal = calificaciones[id];
        
        // Validar campos requeridos
        if (!cal.profesor) {
          errores.push("Calificación " + id + ": profesor vacío");
        }
        if (!cal.alumno) {
          errores.push("Calificación " + id + ": alumno vacío");
        }
        if (!cal.correoAlumno) {
          advertencias.push("Calificación " + id + ": sin correo de alumno");
        }
        if (!cal.grupo) {
          errores.push("Calificación " + id + ": grupo vacío");
        }
        if (!cal.materia) {
          advertencias.push("Calificación " + id + ": sin materia especificada");
        }
        
        // Validar valores de calificación
        var calif = parseFloat(cal.calificacion);
        if (isNaN(calif) || calif < 0 || calif > 10) {
          errores.push("Calificación " + id + ": valor inválido (" + cal.calificacion + ")");
        }
      }
      
      return {
        ok: true,
        totalCalificaciones: Object.keys(calificaciones).length,
        errores: errores,
        advertencias: advertencias,
        esValido: errores.length === 0
      };
      
    } catch (e) {
      console.error("❌ Error en validarIntegridad: " + e.toString());
      return { ok: false, error: e.toString() };
    }
  },
  
  // ════════════════════════════════════════════════════════════════
  // 4. EXPORTAR DATOS
  // ════════════════════════════════════════════════════════════════
  
  /**
   * Exporta calificaciones a un Google Sheet para respaldo
   */
  exportarASheet: function(nombreSheet) {
    nombreSheet = nombreSheet || "RESPALDO_" + new Date().toISOString().split('T')[0];
    
    console.log("📥 Exportando datos a Sheet: " + nombreSheet);
    
    try {
      var config = _getFirebaseConfig();
      var url = config.url + "calificaciones.json?auth=" + config.secret;
      
      var response = UrlFetchApp.fetch(url, { muteHttpExceptions: true });
      
      if (response.getResponseCode() !== 200) {
        return { ok: false, error: "Sin datos para exportar" };
      }
      
      var calificaciones = JSON.parse(response.getContentText());
      
      // Crear nuevo Sheet
      var ss = SpreadsheetApp.getActiveSpreadsheet();
      var sheet = ss.insertSheet(nombreSheet);
      
      // Encabezados
      var headers = [
        "ID", "Profesor", "Matrícula", "Alumno", "Correo", 
        "Grupo", "Materia", "Calificación", "Actividad", "Fecha", "Sync"
      ];
      
      sheet.appendRow(headers);
      
      // Datos
      var filas = [];
      for (var id in calificaciones) {
        var cal = calificaciones[id];
        filas.push([
          id,
          cal.profesor || "",
          cal.matriculaProfesor || "",
          cal.alumno || "",
          cal.correoAlumno || "",
          cal.grupo || "",
          cal.materia || "",
          cal.calificacion || 0,
          cal.actividad || "",
          cal.fecha_actividad || "",
          cal.fecha_carga || ""
        ]);
      }
      
      if (filas.length > 0) {
        sheet.getRange(2, 1, filas.length, headers.length).setValues(filas);
      }
      
      console.log("✅ Respaldo creado con " + filas.length + " registros");
      
      return {
        ok: true,
        mensaje: "Respaldo creado: " + nombreSheet,
        registros: filas.length
      };
      
    } catch (e) {
      console.error("❌ Error en exportarASheet: " + e.toString());
      return { ok: false, error: e.toString() };
    }
  },
  
  // ════════════════════════════════════════════════════════════════
  // 5. OBTENER REPORTE POR PROFESOR
  // ════════════════════════════════════════════════════════════════
  
  /**
   * Obtiene resumen de actividad de cada profesor
   */
  getActividadProfesores: function() {
    console.log("📊 Generando reporte de actividad por profesor");
    
    try {
      var auditoria = AdminPanel.getAuditoriaCargas(30);
      
      if (!auditoria.ok) {
        return { ok: false, error: auditoria.error };
      }
      
      var actividadMap = {};
      
      auditoria.cargas.forEach(function(carga) {
        var matricula = carga.matricula;
        
        if (!actividadMap[matricula]) {
          actividadMap[matricula] = {
            profesor: carga.profesor,
            matricula: matricula,
            totalCargas: 0,
            totalRegistros: 0,
            grupos: new Set(),
            materias: new Set(),
            ultimaCarga: null
          };
        }
        
        actividadMap[matricula].totalCargas++;
        actividadMap[matricula].totalRegistros += carga.registrosAgregados || 0;
        
        if (carga.grupos) {
          carga.grupos.forEach(function(g) {
            actividadMap[matricula].grupos.add(g);
          });
        }
        
        if (carga.materias) {
          carga.materias.forEach(function(m) {
            actividadMap[matricula].materias.add(m);
          });
        }
        
        if (!actividadMap[matricula].ultimaCarga || new Date(carga.timestamp) > new Date(actividadMap[matricula].ultimaCarga)) {
          actividadMap[matricula].ultimaCarga = carga.timestamp;
        }
      });
      
      // Convertir Sets a arrays
      var actividad = [];
      for (var matricula in actividadMap) {
        var prof = actividadMap[matricula];
        actividad.push({
          profesor: prof.profesor,
          matricula: prof.matricula,
          totalCargas: prof.totalCargas,
          totalRegistros: prof.totalRegistros,
          gruposProcesados: Array.from(prof.grupos).length,
          materiasProcesadas: Array.from(prof.materias).length,
          ultimaCarga: prof.ultimaCarga
        });
      }
      
      // Ordenar por última carga
      actividad.sort(function(a, b) {
        return new Date(b.ultimaCarga) - new Date(a.ultimaCarga);
      });
      
      return {
        ok: true,
        totalProfesoresActivos: actividad.length,
        profesores: actividad
      };
      
    } catch (e) {
      console.error("❌ Error en getActividadProfesores: " + e.toString());
      return { ok: false, error: e.toString() };
    }
  }
  
};
