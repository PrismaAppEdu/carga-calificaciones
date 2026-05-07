/**
 * ================================================================
 * SyncServiceOrchestrator.gs — ORQUESTA TODO EL SISTEMA
 * 
 * PROPÓSITO: Coordina lectura (DataReaderRTDB) + guardado (DataPersistenceRTDB)
 * Sin duplicar código. Una interfaz única para sincronización.
 * 
 * USO: SyncService.cargarDatosProfesor(), guardarCalificaciones(), etc.
 * ================================================================
 */

var SyncService = {
  
  // ════════════════════════════════════════════════════════════════
  // 1. CARGAR DATOS PARA UN PROFESOR
  // ════════════════════════════════════════════════════════════════
  
  /**
   * Orquesta: Lee datos del profesor desde RTDB fuente
   * Retorna: { profesor, grupos, alumnos, asignaturas }
   */
  cargarDatosProfesor: function(matriculaProfesor) {
    console.log("📖 Orquestador: cargando datos para profesor " + matriculaProfesor);
    
    try {
      var resultado = DataReaderRTDB.getDataCompletoProfesor(matriculaProfesor);
      
      if (!resultado.ok) {
        return {
          ok: false,
          error: resultado.error
        };
      }
      
      console.log("✅ Datos cargados:");
      console.log("   - Profesor: " + resultado.profesor.nombre);
      console.log("   - Grupos: " + resultado.grupos.length);
      console.log("   - Alumnos: " + resultado.alumnos.length);
      console.log("   - Asignaturas: " + resultado.asignaturas.length);
      
      return resultado;
      
    } catch (e) {
      console.error("❌ Error en cargarDatosProfesor: " + e.toString());
      return { ok: false, error: e.toString() };
    }
  },
  
  // ════════════════════════════════════════════════════════════════
  // 2. GUARDAR CALIFICACIÓN INDIVIDUAL
  // ════════════════════════════════════════════════════════════════
  
  /**
   * Orquesta: Guarda 1 calificación + índices + metadatos
   */
  guardarCalificacion: function(calificacionData) {
    console.log("💾 Orquestador: guardando calificación");
    
    try {
      // 1. Guardar calificación principal
      var resultado1 = DataPersistenceRTDB.saveCalificacion(calificacionData);
      if (!resultado1.ok) {
        return resultado1;
      }
      
      // 2. Guardar índices (para búsquedas rápidas)
      var resultado2 = DataPersistenceRTDB.saveIndices(calificacionData);
      
      console.log("✅ Calificación guardada y indexada");
      return { ok: true, id: resultado1.id };
      
    } catch (e) {
      console.error("❌ Error en guardarCalificacion: " + e.toString());
      return { ok: false, error: e.toString() };
    }
  },
  
  // ════════════════════════════════════════════════════════════════
  // 3. GUARDAR LOTE DE CALIFICACIONES
  // ════════════════════════════════════════════════════════════════
  
  /**
   * Orquesta: Guarda múltiples calificaciones en un solo paso
   * + estructura jerárquica + metadatos
   */
  guardarCalificacionesLote: function(calificacionesArray, matriculaProfesor, nombreProfesor, estadisticas) {
    console.log("💾 Orquestador: guardando lote de " + calificacionesArray.length + " calificaciones");
    
    try {
      // 1. Guardar lote principal
      var resultado1 = DataPersistenceRTDB.saveCalificacionesLote(calificacionesArray);
      if (!resultado1.ok) {
        return resultado1;
      }
      
      // 2. Guardar en estructura jerárquica (por profesor > grupo > materia)
      var estructurado = SyncService._construirEstructuraJerarquica(calificacionesArray);
      var resultado2 = DataPersistenceRTDB.saveCalificacionesEstructurado(matriculaProfesor, estructurado);
      
      // 3. Guardar metadatos
      var resultado3 = DataPersistenceRTDB.saveMetaCarga(matriculaProfesor, nombreProfesor, estadisticas);
      
      console.log("✅ Lote guardado completamente");
      return { ok: true, cantidad: resultado1.cantidad };
      
    } catch (e) {
      console.error("❌ Error en guardarCalificacionesLote: " + e.toString());
      return { ok: false, error: e.toString() };
    }
  },
  
  // ════════════════════════════════════════════════════════════════
  // 4. SINCRONIZACIÓN COMPLETA (Read + Write)
  // ════════════════════════════════════════════════════════════════
  
  /**
   * Orquesta: Leer calificaciones desde Sheets + guardar en Firebase DESTINO
   * Esto es lo que ejecuta el flujo completo del sistema
   */
  sincronizarCalificacionesCompleta: function(datosImportacion) {
    console.log("🔄 SINCRONIZACIÓN COMPLETA: Iniciando...");
    
    try {
      var profesor = datosImportacion.profesor;
      var matricula = datosImportacion.matricula;
      var tipoGrupo = datosImportacion.tipoGrupo;
      var gruposLinks = datosImportacion.gruposLinks;
      
      // 1. LEER calificaciones desde Sheets (ya existe en Code.gs)
      console.log("📖 Paso 1: Leyendo desde Google Sheets...");
      var datosSheets = SyncService._leerCalificacionesDesdeSheets(profesor, tipoGrupo, gruposLinks);
      
      if (!datosSheets.ok) {
        return { ok: false, error: "Error leyendo Sheets: " + datosSheets.error };
      }
      
      // 2. TRANSFORMAR datos al formato de destino
      console.log("🔄 Paso 2: Transformando datos...");
      var calificacionesTransformadas = SyncService._transformarDatos(
        datosSheets.calificaciones,
        matricula,
        profesor
      );
      
      // 3. GUARDAR en RTDB destino
      console.log("💾 Paso 3: Guardando en Firebase destino...");
      var estadisticas = {
        grupos: datosSheets.grupos,
        materias: datosSheets.materias,
        totalRegistros: calificacionesTransformadas.length,
        totalAlumnos: datosSheets.alumnosUnicos
      };
      
      var resultado = SyncService.guardarCalificacionesLote(
        calificacionesTransformadas,
        matricula,
        profesor,
        estadisticas
      );
      
      if (!resultado.ok) {
        return { ok: false, error: "Error guardando: " + resultado.error };
      }
      
      console.log("✅ SINCRONIZACIÓN COMPLETA EXITOSA");
      return {
        ok: true,
        mensaje: "✅ Se sincronizaron " + resultado.cantidad + " calificaciones",
        estadisticas: estadisticas
      };
      
    } catch (e) {
      console.error("❌ Error en sincronizarCalificacionesCompleta: " + e.toString());
      return { ok: false, error: e.toString() };
    }
  },
  
  // ════════════════════════════════════════════════════════════════
  // 5. HELPERS PRIVADOS (INTERNOS)
  // ════════════════════════════════════════════════════════════════
  
  /**
   * Lee calificaciones desde Google Sheets usando la función existente
   */
  _leerCalificacionesDesdeSheets: function(profesor, tipoGrupo, gruposLinks) {
    try {
      // Aquí se ejecuta la importación existente de Code.gs
      var resultado = importarGruposWeb({
        profesor: profesor,
        tipoGrupo: tipoGrupo,
        gruposLinks: gruposLinks
      });
      
      if (!resultado.ok) {
        return { ok: false, error: resultado.error };
      }
      
      // Retornar datos leídos
      return {
        ok: true,
        calificaciones: resultado.calificaciones || [],
        grupos: resultado.grupos || [],
        materias: resultado.materias || [],
        alumnosUnicos: resultado.alumnosUnicos || 0
      };
      
    } catch (e) {
      console.error("Error en _leerCalificacionesDesdeSheets: " + e.toString());
      return { ok: false, error: e.toString() };
    }
  },
  
  /**
   * Transforma calificaciones al formato destino
   */
  _transformarDatos: function(calificacionesOrigen, matricula, profesor) {
    return calificacionesOrigen.map(function(cal) {
      return {
        profesor: profesor,
        matriculaProfesor: matricula,
        alumno: cal.alumno || "",
        correoAlumno: cal.correo || "",
        grupo: cal.grupo || "",
        materia: cal.asignatura || "",
        calificacion: parseFloat(cal.calificacion) || 0,
        fecha_actividad: cal.fecha_act || "",
        actividad: cal.actividad || ""
      };
    });
  },
  
  /**
   * Construye estructura jerárquica: profesor > grupo > materia > alumno
   */
  _construirEstructuraJerarquica: function(calificacionesArray) {
    var estructura = {};
    
    calificacionesArray.forEach(function(cal) {
      var grupoKey = cal.grupo.toUpperCase();
      var materiaKey = (cal.materia || "SIN MATERIA").substring(0, 3).toUpperCase();
      var correoKey = _sanitizarClave(cal.correoAlumno);
      
      if (!estructura[grupoKey]) {
        estructura[grupoKey] = {};
      }
      if (!estructura[grupoKey][materiaKey]) {
        estructura[grupoKey][materiaKey] = {};
      }
      if (!estructura[grupoKey][materiaKey][correoKey]) {
        estructura[grupoKey][materiaKey][correoKey] = {
          nombre: cal.alumno,
          correo: cal.correoAlumno,
          calificaciones: []
        };
      }
      
      estructura[grupoKey][materiaKey][correoKey].calificaciones.push({
        calificacion: cal.calificacion,
        actividad: cal.actividad,
        fecha: cal.fecha_actividad,
        sync: new Date().toISOString()
      });
    });
    
    return estructura;
  }
  
};
