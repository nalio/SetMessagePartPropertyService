package com.progress.codeshare.esbservice.setMessagePartProperty;

import java.io.StringReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathFactory;

import net.sf.saxon.om.NamespaceConstant;
import net.sf.saxon.sxpath.IndependentContext;
import net.sf.saxon.xpath.NamespaceContextImpl;

import org.xml.sax.InputSource;

import com.sonicsw.xq.XQConstants;
import com.sonicsw.xq.XQEnvelope;
import com.sonicsw.xq.XQHeader;
import com.sonicsw.xq.XQInitContext;
import com.sonicsw.xq.XQMessage;
import com.sonicsw.xq.XQParameters;
import com.sonicsw.xq.XQPart;
import com.sonicsw.xq.XQServiceContext;
import com.sonicsw.xq.XQServiceEx;
import com.sonicsw.xq.XQServiceException;

public final class SetMessagePartPropertyService implements XQServiceEx {
	private static final String MODE_XPATH = "XPath";
	private static final String MODE_DATE_TIME = "Date & Time";
	private static final String MODE_CONTENT = "Content";
	private static final String MODE_CONSTANT = "Constant";
	private static final DateFormat DATE_FORMAT = new SimpleDateFormat();
	private static final String PARAM_CONST_VAL = "constVal";
	private static final String PARAM_DATE_TIME_VAL = "dateTimeVal";
	private static final String PARAM_MESSAGE_PART = "messagePart";
	private static final String PARAM_MODE = "mode";
	private static final String PARAM_NAME = "name";
	private static final String PARAM_NAMESPACES = "namespaces";
	private static final String PARAM_XPATH_EXPR = "xpathExpr";
	private static final Pattern PATTERN_NAMESPACE = Pattern
			.compile("([-._:A-Za-z0-9]*)=([^,]*),?");

	public void destroy() {
	}

	public void init(XQInitContext ctx) {
	}

	public void service(final XQServiceContext ctx) throws XQServiceException {

		try {
			final XQParameters params = ctx.getParameters();

			final String mode = params.getParameter(PARAM_MODE,
					XQConstants.PARAM_STRING);
			final int messagePart = params.getIntParameter(PARAM_MESSAGE_PART,
					XQConstants.PARAM_STRING);
			final String name = params.getParameter(PARAM_NAME,
					XQConstants.PARAM_STRING);

			if (MODE_CONSTANT.equals(mode)) {
				final String constVal = params.getParameter(PARAM_CONST_VAL,
						XQConstants.PARAM_STRING);

				if (constVal != null) {

					while (ctx.hasNextIncoming()) {
						final XQEnvelope env = ctx.getNextIncoming();

						final XQMessage msg = env.getMessage();

						for (int i = 0; i < msg.getPartCount(); i++) {

							/* Decide whether to process the part or not */
							if ((messagePart == i)
									|| (messagePart == XQConstants.ALL_PARTS)) {
								final XQPart part = msg.getPart(i);

								final XQHeader header = part.getHeader();

								header.setValue(name, constVal);

								msg.replacePart(part, i);
							}

							/* Break when done */
							if (messagePart == i)
								break;

						}

						final Iterator addressIterator = env.getAddresses();

						if (addressIterator.hasNext())
							ctx.addOutgoing(env);

					}

				}

			} else if (MODE_CONTENT.equals(mode)) {

				while (ctx.hasNextIncoming()) {
					final XQEnvelope env = ctx.getNextIncoming();

					final XQMessage msg = env.getMessage();

					for (int i = 0; i < msg.getPartCount(); i++) {

						/* Decide whether to process the part or not */
						if ((messagePart == i)
								|| (messagePart == XQConstants.ALL_PARTS)) {
							final XQPart part = msg.getPart(i);

							final XQHeader header = part.getHeader();

							header.setValue(name, (String) part.getContent());

							msg.replacePart(part, i);
						}

						/* Break when done */
						if (messagePart == i)
							break;

					}

					final Iterator addressIterator = env.getAddresses();

					if (addressIterator.hasNext())
						ctx.addOutgoing(env);

				}

			} else if (MODE_DATE_TIME.equals(mode)) {
				final String dateTimeVal = params.getParameter(
						PARAM_DATE_TIME_VAL, XQConstants.PARAM_STRING);

				final Date dateTime = DATE_FORMAT.parse(dateTimeVal);

				while (ctx.hasNextIncoming()) {
					final XQEnvelope env = ctx.getNextIncoming();

					final XQMessage msg = env.getMessage();

					for (int i = 0; i < msg.getPartCount(); i++) {

						/* Decide whether to process the part or not */
						if ((messagePart == i)
								|| (messagePart == XQConstants.ALL_PARTS)) {
							final XQPart part = msg.getPart(i);

							final XQHeader header = part.getHeader();

							header.setValue(name, dateTime.toString());

							msg.replacePart(part, i);
						}

						/* Break when done */
						if (messagePart == i)
							break;

					}

					final Iterator addressIterator = env.getAddresses();

					if (addressIterator.hasNext())
						ctx.addOutgoing(env);

				}

			} else if (MODE_XPATH.equals(mode)) {
				final String xpathExpr = params.getParameter(PARAM_XPATH_EXPR,
						XQConstants.PARAM_STRING);

				if (xpathExpr != null) {
					final XPathFactory factory = XPathFactory
							.newInstance(NamespaceConstant.OBJECT_MODEL_SAXON);

					final XPath xpath = factory.newXPath();

					final String namespaces = params.getParameter(
							PARAM_NAMESPACES, XQConstants.PARAM_STRING);

					if (namespaces != null) {
						final Matcher matcher = PATTERN_NAMESPACE
								.matcher(namespaces);

						/* Configure the namespaces */
						final IndependentContext resolver = new IndependentContext();

						while (matcher.find())
							resolver.declareNamespace(matcher.group(1), matcher
									.group(2));

						xpath.setNamespaceContext(new NamespaceContextImpl(
								resolver));
					}

					while (ctx.hasNextIncoming()) {
						final XQEnvelope env = ctx.getNextIncoming();

						final XQMessage msg = env.getMessage();

						for (int i = 0; i < msg.getPartCount(); i++) {

							/* Decide whether to process the part or not */
							if ((messagePart == i)
									|| (messagePart == XQConstants.ALL_PARTS)) {
								final XQPart part = msg.getPart(i);

								final XQHeader header = part.getHeader();

								header.setValue(name, xpath.evaluate(xpathExpr,
										new InputSource(new StringReader(
												(String) part.getContent()))));

								msg.replacePart(part, i);
							}

							/* Break when done */
							if (messagePart == i)
								break;

						}

						final Iterator addressIterator = env.getAddresses();

						if (addressIterator.hasNext())
							ctx.addOutgoing(env);

					}

				}

			}

		} catch (final Exception e) {
			throw new XQServiceException(e);
		}

	}

	public void start() {
	}

	public void stop() {
	}

}