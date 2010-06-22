/*
 * @file cllsd.c
 * @brief Accelerated XML generation for LLSD
 * @author Bryan O'Sullivan
 * 
 * $LicenseInfo:firstyear=2008&license=mit$
 * 
 * Copyright (c) 2008-2009, Linden Research, Inc.
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 * $/LicenseInfo$
 *
 * Note: this module is not compatible with the type trickery played
 * by saranwrap.
 */

#include <Python.h>
#include <stdlib.h>
#include <string.h>

#if PY_MAJOR_VERSION == 2 && PY_MINOR_VERSION < 5
typedef int Py_ssize_t;
#endif

typedef struct 
{
	char *ptr;
	Py_ssize_t off;
	Py_ssize_t size;
} Buffer;

// Not a fuzzy estimation method. If we still have core dumps
// due to realloc attempts, try reducing below number to 1.5
#define MEM_FACTOR 2.0

static inline int buf_ensure(Buffer *buf, Py_ssize_t len)
{
    //DEV-14630 - Incorporate python module. Addressing core dump issue
    int asymptotic_len = fmax(buf->size * MEM_FACTOR, buf->size + len);

	//fprintf(stderr, "\nBuffer : %s\nBuffer Size: %d\nBuffer Offset: %d\nLength: %d\nMax(%f, %d): %d\n", 
    //        buf->ptr, buf->size, buf->off, len, buf->size * MEM_FACTOR, buf->size + len, asymptotic_len);
	if (len > buf->size - buf->off) {
		char *nptr = realloc(buf->ptr, asymptotic_len);
		if (nptr == NULL) {
			PyErr_SetString(PyExc_MemoryError, "out of memory");
			return 0;
		}
		buf->ptr = nptr;
		buf->size = asymptotic_len;
	}

	return 1;
}

static inline void buf_append(Buffer *buf, const char *str, Py_ssize_t len)
{
	memcpy(buf->ptr + buf->off, str, len);
	buf->off += len;
}

static inline void buf_char_append(Buffer *buf, char c)
{
	buf->ptr[buf->off++] = c;
}

static inline int buf_extend(Buffer *buf, const char *str, Py_ssize_t len)
{
	if (!buf_ensure(buf, len))
		return 0;

	buf_append(buf, str, len);

	return 1;
}

static int esc_extend(Buffer *buf, const char *str, Py_ssize_t len)
{
	Py_ssize_t i, excess;

	if (!buf_ensure(buf, len))
		return 0;

	for (i = excess = 0; i < len; i++) {
		switch (str[i]) {
		case '&':
			excess += 4;
			if (!buf_ensure(buf, len - i + excess))
				return 0;
			buf_append(buf, "&amp;", 5);
			break;
		case '<':
			excess += 3;
			if (!buf_ensure(buf, len - i + excess))
				return 0;
			buf_append(buf, "&lt;", 4);
			break;
		case '>':
			excess += 3;
			if (!buf_ensure(buf, len - i + excess))
				return 0;
			buf_append(buf, "&gt;", 4);
			break;
		default:
			buf_char_append(buf, str[i]); 
			break;
		}
	}

	return 1;
}

static inline PyObject *as_string(PyObject *obj)
{
	PyObject *strobj;

	if (PyString_Check(obj)) {
		strobj = obj;
		Py_INCREF(strobj);
	} else if (PyUnicode_Check(obj)) {
		strobj = PyUnicode_AsUTF8String(obj);
		if (!strobj)
			goto bail;
	} else {
		strobj = PyObject_Str(obj);
		if (!strobj)
			goto bail;

		if (!PyString_Check(strobj)) {
			Py_DECREF(strobj);
			strobj = NULL;
			PyErr_SetString(PyExc_TypeError,
					"str() did not return a string");
		}
	}

bail:
	return strobj;
}

static int obj_to_xml(Buffer *buf, const char *name, PyObject *obj)
{
	PyObject *strobj;
	size_t namelen;
	Py_ssize_t len;
	int ret = 0;
	char *str;

	strobj = as_string(obj);
	if (strobj == NULL)
		goto bail;

	len = PyString_GET_SIZE(strobj);
	str = PyString_AS_STRING(strobj);

	namelen = strlen(name);

	if (!buf_ensure(buf, namelen * 2 + 5 + len))
		goto bail;

	buf_char_append(buf, '<');
	buf_append(buf, name, namelen);
	buf_char_append(buf, '>');
	buf_append(buf, str, len);
	buf_append(buf, "</", 2);
	buf_append(buf, name, namelen);
	buf_char_append(buf, '>');

	ret = 1;

bail:
	Py_XDECREF(strobj);
	return ret;
}

static int float_to_xml(Buffer *buf, PyObject *obj)
{
	double val = PyFloat_AS_DOUBLE(obj);

	if (val == 0.0)
		return buf_extend(buf, "<real/>", 7);

	if (!buf_ensure(buf, 120))
		return 0;
		
	buf_append(buf, "<real>", 6);
	PyFloat_AsString(buf->ptr + buf->off,
			 (PyFloatObject *) obj);
	buf->off += strlen(buf->ptr + buf->off);
	buf_append(buf, "</real>", 7);

	return 1;
}

static int datetime_to_xml(Buffer *buf, PyObject *obj)
{
	int has_ms = 0;
	PyObject* isoobj = NULL;
	PyObject* strobj = NULL;
	char* str;
	Py_ssize_t len;
	int ret = 0;

	// Try to get out the microsecond value from obj. If that succeeds
	// then we need to use isoformat to get fractional
	// seconds. 2009-02-02 Phoenix
	has_ms = PyObject_HasAttrString(obj, "microsecond");
	if(has_ms)
	{
		isoobj = PyObject_CallMethod(obj, "isoformat", "()");
	}
	else
	{
		isoobj = PyObject_CallMethod(
			obj,
			"strftime",
			"s",
			"%Y-%m-%dT%H:%M:%S");
	}
	if (isoobj == NULL)
		goto bail;

	strobj = as_string(isoobj);
	if (strobj == NULL)
		goto bail;

	// The magic number used here is the length of the minimal date
	// string you can get from 'YYYY-MM-DDTHH:MM:SS' which is exactly
	// 19 bytes. If we used isoformat above then this string is
	// probably longer.
	len = PyString_GET_SIZE(strobj);
	str = PyString_AS_STRING(strobj);
	if (len < 19)
	{
		ret = buf_extend(buf, "<date/>", 7);
		goto bail;
	}

	buf_extend(buf, "<date>", 6);
	buf_extend(buf, str, len);
	buf_extend(buf, "Z</date>", 8);
	ret = 1;

bail:
	Py_XDECREF(isoobj);
	Py_XDECREF(strobj);
	return ret;
}

static int string_to_xml(Buffer *buf, PyObject *obj)
{
	Py_ssize_t len;

	len = PyString_GET_SIZE(obj);

	if (len) {
		if (!buf_extend(buf, "<string>", 8))
			return 0;
		if (!esc_extend(buf, PyString_AS_STRING(obj), len))
			return 0;
		return buf_extend(buf, "</string>", 9);
	}

	return buf_extend(buf, "<string/>", 9);
}

static int int_to_xml(Buffer *buf, PyObject *obj)
{
	long val = PyInt_AS_LONG(obj);

	if (val == 0)
		return buf_extend(buf, "<integer/>", 10);
	
	if (!buf_ensure(buf, 64))
		return 0;

	buf->off += PyOS_snprintf(buf->ptr + buf->off, 64,
				  "<integer>%ld</integer>", val);

	return 1;
}

static inline int is_module_type(PyObject *obj, const char *modulename,
				const char *typename)
{
	PyObject *mod;
	int ret = 0;

	mod = PyImport_ImportModule(modulename);
	if (mod != NULL) {
		PyObject *type;

		type = PyObject_GetAttrString(mod, typename);
		if (type) {
			ret = PyObject_IsInstance(obj, type);
			Py_DECREF(type);
		}
	}
	Py_XDECREF(mod);
	return ret;
}

static int binary_to_xml(Buffer *buf, PyObject *obj)
{
	PyObject *mod;
	PyObject *base64 = NULL;
	int ret = 0;

	mod = PyImport_ImportModule("binascii");
	if (mod == NULL)
		goto bail;
	
	base64 = PyObject_CallMethod(mod, "b2a_base64", "O", obj);
	if (base64)
		ret = obj_to_xml(buf, "binary", base64);

bail:
	Py_XDECREF(base64);
	Py_XDECREF(mod);
	return ret;
}

static int bool_to_xml(Buffer *buf, PyObject *obj)
{
	if (obj == Py_True)
		return buf_extend(buf, "<boolean>true</boolean>", 23);
	else if (obj == Py_False)
		return buf_extend(buf, "<boolean>false</boolean>", 24);

	PyErr_SetString(PyExc_TypeError, "impossible bool value");
	return 0;
}

static int uri_to_xml(Buffer *buf, PyObject *obj)
{
	PyObject *strobj;
	int ret = 0;

	strobj = as_string(obj);
	if (strobj == NULL)
		goto bail;
	
	if (!buf_extend(buf, "<uri>", 5))
		goto bail;

	if (!esc_extend(buf, PyString_AS_STRING(strobj),
			PyString_GET_SIZE(strobj)))
		goto bail;
	
	if (!buf_extend(buf, "</uri>", 6))
		goto bail;

	ret = 1;
bail:
	Py_XDECREF(strobj);
	return ret;
}

static int any_to_xml(Buffer *buf, PyObject *obj);

static int seq_to_xml(Buffer *buf, PyObject *obj)
{
	Py_ssize_t len, i;

	len = PySequence_Size(obj);

	if (len == 0)
		return buf_extend(buf, "<array/>", 8);
		
	if (!buf_extend(buf, "<array>", 7))
		return 0;

	for (i = 0; i < len; i++) {
		if (!any_to_xml(buf, PySequence_Fast_GET_ITEM(obj, i)))
			return 0;
	}

	return buf_extend(buf, "</array>", 8);
}

static int iter_to_xml(Buffer *buf, PyObject *obj)
{
	PyObject *iter, *cur = NULL;
	int ret = 0;

	iter = PyObject_GetIter(obj);
	if (iter == NULL)
		goto bail;

	cur = PyIter_Next(iter);

	if (cur == NULL) {
		ret = buf_extend(buf, "<array/>", 8);
		goto bail;
	}
	
	if (!buf_extend(buf, "<array>", 7))
		goto bail;

	do {
		ret = any_to_xml(buf, cur);
		if (!ret)
			goto bail;
		Py_DECREF(cur);
		cur = PyIter_Next(iter);
	} while (cur != NULL);
	ret = buf_extend(buf, "</array>", 8);

bail:
	Py_XDECREF(cur);
	Py_XDECREF(iter);
	return ret;
}

static int dict_to_xml(Buffer *buf, PyObject *obj)
{
	PyObject *key, *value;
	Py_ssize_t pos = 0;

	if (PyDict_Size(obj) == 0)
		return buf_extend(buf, "<map/>", 6);
	
	if (!buf_extend(buf, "<map>", 5))
		return 0;
	
	while (PyDict_Next(obj, &pos, &key, &value)) {
		PyObject *strobj;

		strobj = as_string(key);
		if (strobj == NULL)
			return 0;
		
		if (!buf_extend(buf, "<key>", 5))
			return 0;

		if (!esc_extend(buf, PyString_AS_STRING(strobj),
				PyString_GET_SIZE(strobj)))
			return 0;
		Py_DECREF(strobj);

		if (!buf_extend(buf, "</key>", 6))
			return 0;

		if (!any_to_xml(buf, value))
			return 0;
	}

	if (!buf_extend(buf, "</map>", 6))
		return 0;

	return 1;
}

static int LLSD_to_xml(Buffer *buf, PyObject *obj)
{
	PyObject *thing;
	int c = 0;

	thing = PyObject_GetAttrString(obj, "thing");
	if (thing == NULL)
		goto bail;

	c = any_to_xml(buf, thing);

 bail:
	Py_XDECREF(thing);
	return c;
}

static int LLUUID_to_xml(Buffer *buf, PyObject *obj)
{
	PyObject *isNull = NULL, *strobj = NULL;
	int ret = 0;

	isNull = PyObject_CallMethod(obj, "isNull", "()");

	if (isNull == Py_True) {
		ret = buf_extend(buf, "<uuid/>", 7);
		goto bail;
	}

	strobj = PyObject_CallMethod(obj, "toString", "()");
	if (strobj == NULL)
		goto bail;

	ret = obj_to_xml(buf, "uuid", strobj);

bail:
	Py_XDECREF(isNull);
	Py_XDECREF(strobj);
	return ret;
}

static int unicode_to_xml(Buffer *buf, PyObject *obj)
{
	PyObject *strobj;
	int ret = 0;
	
	strobj = PyUnicode_AsUTF8String(obj);

	if (strobj == NULL)
		goto bail;

	ret = string_to_xml(buf, strobj);
bail:
	Py_XDECREF(strobj);
	return ret;
}

static int long_to_xml(Buffer *buf, PyObject *obj)
{
	long val = PyLong_AsLong(obj);
	PyObject *err = PyErr_Occurred();

	if (err != NULL)
		PyErr_Clear();

	if (val != 0 || err != NULL)
		return obj_to_xml(buf, "integer", obj);

	return buf_extend(buf, "<integer/>", 10);
}

static int any_to_xml(Buffer *buf, PyObject *obj)
{
	if (PyDict_Check(obj))
		return dict_to_xml(buf, obj);

	/*
	 * Do an exact string check first, deferring the more general
	 * (and less likely) string check until after the expensive
	 * and improbable checks for uri and binary.
	 */
	if (PyString_CheckExact(obj))
		return string_to_xml(buf, obj);

	if (PyUnicode_Check(obj))
		return unicode_to_xml(buf, obj);

	if (PyBool_Check(obj))
		return bool_to_xml(buf, obj);

	if (PyInt_Check(obj))
		return int_to_xml(buf, obj);

	if (PyLong_Check(obj))
		return long_to_xml(buf, obj);

	if (PyFloat_Check(obj))
		return float_to_xml(buf, obj);

	if (obj == Py_None)
		return buf_extend(buf, "<undef/>", 8);

	/* Don't use PySequence_Check here!  It's too general. */

	if (PyList_Check(obj) || PyTuple_Check(obj))
		return seq_to_xml(buf, obj);
	
	/*
	 * These checks must occur before the more general check for
	 * strings, or it will erroneously match them because they're
	 * subclasses of str.
	 */

	if (is_module_type(obj, "indra.base.llsd", "uri"))
		return uri_to_xml(buf, obj);

	if (is_module_type(obj, "indra.base.llsd", "binary"))
		return binary_to_xml(buf, obj);

	if (PyString_Check(obj))
		return string_to_xml(buf, obj);
	
	/*
	 * Check for something iterable after we've exhausted all more
	 * specific possibilities that support iteration.
	 */
	if (PyIter_Check(obj))
		return iter_to_xml(buf, obj);

	if (is_module_type(obj, "datetime", "datetime") || 
		is_module_type(obj, "datetime", "date"))
		return datetime_to_xml(buf, obj);
	
	if (is_module_type(obj, "indra.base.llsd", "LLSD"))
		return LLSD_to_xml(buf, obj);

	if (is_module_type(obj, "indra.base.lluuid", "UUID"))
		return LLUUID_to_xml(buf, obj); 

	PyErr_SetString(PyExc_TypeError, "invalid type");
	return 0;
}

static PyObject *llsd_to_xml(PyObject *self, PyObject *args)
{
	PyObject *obj, *ret = NULL;
	Buffer buf;

	buf.ptr = NULL;

	if (!PyArg_ParseTuple(args, "O:llsd_to_xml", &obj))
		goto bail;

	buf.size = 256;
	buf.ptr = malloc(buf.size);

	if (buf.ptr == NULL)
	{
		PyErr_SetString(PyExc_MemoryError, "out of memory");
		goto bail;
	}

	buf.off = 0;

	buf_extend(&buf, "<?xml version=\"1.0\" ?><llsd>", 28);
	if (!any_to_xml(&buf, obj))
		goto bail;
	if (!buf_extend(&buf, "</llsd>", 7))
		goto bail;

	ret = PyString_FromStringAndSize(buf.ptr, buf.off);

	goto done;

bail:
	Py_XDECREF(ret);
	Py_XDECREF(obj);
	ret = NULL;

done:
	if (buf.ptr)
		free(buf.ptr);
	return ret;
}

static char cllsd_doc[] = "Efficient LLSD parsing.";

static PyMethodDef methods[] = {
	{"llsd_to_xml", llsd_to_xml, METH_VARARGS,
	 "Represent an LLSD value using XML encoding\n"},
	{NULL, NULL}
};

PyMODINIT_FUNC initcllsd(void)
{
	Py_InitModule3("cllsd", methods, cllsd_doc);
}
